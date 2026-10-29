//! ESPHome native API server exposing a "Streaming" binary sensor, so Home
//! Assistant auto-discovers it (mDNS) and can drive a key light from it.
//!
//! The API encryption key is generated on first start and stored in
//! `KEY_FILE`. On a read-only (overlayfs) rootfs that lands in RAM until copied
//! into the lower layer.

use std::{
    io::Read,
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use base64::{prelude::BASE64_STANDARD, Engine};
use esphome_native_api::{
    esphomeapi::EspHomeApi,
    hash::hash_fnv1,
    parser::ProtoMessage,
    proto::{BinarySensorStateResponse, ListEntitiesBinarySensorResponse, ListEntitiesDoneResponse},
};
use tokio::{net::TcpListener, sync::watch};

const KEY_FILE: &str = "/var/lib/eos-uvc/api_key";
const NAME: &str = "eos-uvc";
const PORT: u16 = 6053;
const OBJECT_ID: &str = "streaming";

/// Spawn the API server on a background thread. Failures are logged, never
/// fatal: the webcam must keep working without Home Assistant.
pub fn spawn(streaming: &'static AtomicBool) {
    thread::Builder::new()
        .name("esphome".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(rt) => rt,
                Err(e) => return eprintln!("esphome: failed to start runtime: {e}"),
            };
            if let Err(e) = rt.block_on(run(streaming)) {
                eprintln!("esphome: disabled: {e:#}");
            }
        })
        .expect("spawn esphome thread");
}

async fn run(streaming: &'static AtomicBool) -> Result<()> {
    let key = load_or_create_key()?;
    let mac = mac_address().unwrap_or_else(|| "00:00:00:00:00:00".into());

    let api = EspHomeApi::builder()
        .name(NAME.to_string())
        .friendly_name("EOS Webcam".to_string())
        .manufacturer("eos-uvc".to_string())
        .model("Raspberry Pi UVC bridge".to_string())
        .mac(mac.clone())
        .encryption_key(key)
        .build()?;

    let listener = TcpListener::bind(("0.0.0.0", PORT)).await.context("bind API port")?;
    let _mdns = advertise(&mac)?; // keep alive: dropping stops advertising
    eprintln!("esphome: listening on :{PORT}");

    // Fan the atomic out to all connections.
    let (state_tx, state_rx) = watch::channel(streaming.load(Ordering::Relaxed));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        loop {
            tick.tick().await;
            state_tx.send_if_modified(|s| {
                let now = streaming.load(Ordering::Relaxed);
                std::mem::replace(s, now) != now
            });
        }
    });

    loop {
        let (stream, peer) = listener.accept().await?;
        let api = api.clone();
        let state_rx = state_rx.clone();
        tokio::spawn(async move {
            if let Err(e) = serve(api, stream, state_rx).await {
                eprintln!("esphome: {peer}: {e:#}");
            }
        });
    }
}

async fn serve(api: EspHomeApi, stream: tokio::net::TcpStream, mut state: watch::Receiver<bool>) -> Result<()> {
    let conn = api.start(stream).await?;
    let tx = conn.sender();
    let mut rx = conn.receiver();
    let key = hash_fnv1(&OBJECT_ID.to_string());
    let state_msg = |s: bool| {
        ProtoMessage::BinarySensorStateResponse(BinarySensorStateResponse {
            key,
            state: s,
            missing_state: false,
            device_id: 0,
        })
    };

    let mut subscribed = false;
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Ok(ProtoMessage::ListEntitiesRequest(_)) => {
                    tx.send(ProtoMessage::ListEntitiesBinarySensorResponse(ListEntitiesBinarySensorResponse {
                        object_id: OBJECT_ID.to_string(),
                        key,
                        name: "Streaming".to_string(),
                        device_class: "running".to_string(),
                        icon: "mdi:webcam".to_string(),
                        ..Default::default()
                    })).await?;
                    tx.send(ProtoMessage::ListEntitiesDoneResponse(ListEntitiesDoneResponse {})).await?;
                }
                Ok(ProtoMessage::SubscribeStatesRequest(_)) => {
                    subscribed = true;
                    let s = *state.borrow_and_update();
                    tx.send(state_msg(s)).await?;
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            },
            changed = state.changed(), if subscribed => {
                changed?;
                let s = *state.borrow_and_update();
                tx.send(state_msg(s)).await?;
            }
        }
    }
}

fn load_or_create_key() -> Result<String> {
    if let Ok(key) = std::fs::read_to_string(KEY_FILE) {
        return Ok(key.trim().to_string());
    }
    let mut raw = [0u8; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut raw)?;
    let key = BASE64_STANDARD.encode(raw);
    if let Some(dir) = std::path::Path::new(KEY_FILE).parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(KEY_FILE, &key).with_context(|| format!("write {KEY_FILE}"))?;
    std::fs::set_permissions(KEY_FILE, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
    eprintln!("esphome: api key: {key}");
    Ok(key)
}

/// Stable MAC of the first real interface; Home Assistant uses it as the device ID.
fn mac_address() -> Option<String> {
    let mut ifaces: Vec<_> = std::fs::read_dir("/sys/class/net").ok()?.flatten().map(|e| e.path()).collect();
    ifaces.sort();
    ifaces.iter().filter(|p| p.join("device").exists()).find_map(|p| {
        let mac = std::fs::read_to_string(p.join("address")).ok()?.trim().to_uppercase();
        (mac != "00:00:00:00:00:00").then_some(mac)
    })
}

fn advertise(mac: &str) -> Result<mdns_sd::ServiceDaemon> {
    let mdns = mdns_sd::ServiceDaemon::new()?;
    let props = [
        ("mac", mac.replace(':', "").to_lowercase()),
        ("version", "2026.6.2".to_string()),
        ("platform", "linux".to_string()),
        ("network", "wifi".to_string()),
        ("api_encryption", "Noise_NNpsk0_25519_ChaChaPoly_SHA256".to_string()),
    ];
    let info = mdns_sd::ServiceInfo::new(
        "_esphomelib._tcp.local.",
        NAME,
        &format!("{NAME}.local."),
        "",
        PORT,
        &props[..],
    )?
    .enable_addr_auto();
    mdns.register(info)?;
    Ok(mdns)
}
