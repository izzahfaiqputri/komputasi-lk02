"""
publisher_cuaca.py
Publisher simulasi 5 stasiun cuaca cerdas
Praktikum Komputasi Terdistribusi

Mengirim 5 pesan/detik ke MQTT broker (1 per stasiun, bergantian)
Topik: stasiun/cuaca/<station_id>
"""

import os
import json
import time
import random
import math
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# ─── Konfigurasi ─────────────────────────────────────────────────────────────
BROKER_HOST    = os.environ.get("MQTT_BROKER", "localhost")
BROKER_PORT    = int(os.environ.get("MQTT_PORT", 1883))
PUBLISH_RATE   = 5        # total pesan per detik (semua stasiun)
INTERVAL       = 1.0 / PUBLISH_RATE  # jeda antar pesan = 0.2 detik

# ─── Definisi 5 Stasiun ───────────────────────────────────────────────────────
STATIONS = [
    {"station_id": "WS-001", "lokasi": "Gedung_A",  "lat": -7.2504, "lon": 112.7688},
    {"station_id": "WS-002", "lokasi": "Gedung_B",  "lat": -7.2512, "lon": 112.7695},
    {"station_id": "WS-003", "lokasi": "Lapangan",  "lat": -7.2498, "lon": 112.7701},
    {"station_id": "WS-004", "lokasi": "Parkiran",  "lat": -7.2520, "lon": 112.7680},
    {"station_id": "WS-005", "lokasi": "Rooftop",   "lat": -7.2508, "lon": 112.7710},
]

# ─── Profil baseline per stasiun ─────────────────────────────────────────────
# Tiap stasiun punya karakteristik berbeda supaya data lebih realistis
STATION_PROFILE = {
    "WS-001": {"suhu_base": 31.0, "aqi_base": 85,  "hujan_prob": 0.05},
    "WS-002": {"suhu_base": 29.5, "aqi_base": 75,  "hujan_prob": 0.05},
    "WS-003": {"suhu_base": 30.0, "aqi_base": 65,  "hujan_prob": 0.08},
    "WS-004": {"suhu_base": 32.0, "aqi_base": 110, "hujan_prob": 0.03},
    "WS-005": {"suhu_base": 33.0, "aqi_base": 95,  "hujan_prob": 0.04},
}

ARAH_ANGIN = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

# ─── State simulasi per stasiun ───────────────────────────────────────────────
# Menyimpan nilai terakhir untuk membuat perubahan bertahap (random walk)
_state: dict = {
    sid: {
        "suhu":   STATION_PROFILE[sid]["suhu_base"],
        "lembab": random.uniform(60, 80),
        "tekanan": random.uniform(1008, 1015),
        "angin":  random.uniform(5, 20),
        "aqi":    float(STATION_PROFILE[sid]["aqi_base"]),
        "lux":    random.uniform(20000, 60000),
        "arah_idx": random.randint(0, 7),
    }
    for sid in STATION_PROFILE
}

# ─── Generator data sensor ────────────────────────────────────────────────────
def generate_payload(station: dict) -> dict:
    sid     = station["station_id"]
    profile = STATION_PROFILE[sid]
    st      = _state[sid]

    # Perubahan bertahap (random walk) agar data terlihat natural
    st["suhu"]    += random.uniform(-0.5, 0.5)
    st["lembab"]  += random.uniform(-1.0, 1.0)
    st["tekanan"] += random.uniform(-0.3, 0.3)
    st["angin"]   += random.uniform(-2.0, 2.0)
    st["aqi"]     += random.uniform(-3.0, 3.0)
    st["lux"]     += random.uniform(-2000, 2000)

    # Sesekali inject kondisi ekstrem (peluang 3%)
    if random.random() < 0.03:
        st["suhu"]  = random.uniform(38.0, 41.0)
    if random.random() < 0.03:
        st["aqi"]   = random.uniform(151, 250)
    if random.random() < 0.03:
        st["angin"] = random.uniform(40, 60)

    # Sesekali hujan
    hujan = 0.0
    if random.random() < profile["hujan_prob"]:
        hujan = random.uniform(5.0, 20.0)

    # Arah angin berubah perlahan
    if random.random() < 0.1:
        st["arah_idx"] = (st["arah_idx"] + random.choice([-1, 0, 1])) % 8

    # Clamp ke rentang normal
    st["suhu"]    = max(15.0,  min(42.0,  st["suhu"]))
    st["lembab"]  = max(30.0,  min(98.0,  st["lembab"]))
    st["tekanan"] = max(995.0, min(1025.0, st["tekanan"]))
    st["angin"]   = max(0.0,   min(60.0,  st["angin"]))
    st["aqi"]     = max(0.0,   min(300.0, st["aqi"]))
    st["lux"]     = max(0.0,   min(100000.0, st["lux"]))

    return {
        "timestamp":       datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "station_id":      sid,
        "lokasi":          station["lokasi"],
        "lat":             station["lat"],
        "lon":             station["lon"],
        "suhu_c":          round(st["suhu"], 1),
        "kelembaban_pct":  round(st["lembab"], 1),
        "tekanan_hpa":     round(st["tekanan"], 1),
        "kecepatan_angin": round(st["angin"], 1),
        "arah_angin":      ARAH_ANGIN[st["arah_idx"]],
        "curah_hujan_mm":  round(hujan, 1),
        "aqi":             int(st["aqi"]),
        "intensitas_lux":  int(st["lux"]),
    }


# ─── MQTT Callbacks ──────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Terhubung ke broker {BROKER_HOST}:{BROKER_PORT}")
        print(f"[MQTT] Mulai publish {PUBLISH_RATE} pesan/detik ke topik stasiun/cuaca/<station_id>")
        print(f"[MQTT] Stasiun aktif: {', '.join(s['station_id'] for s in STATIONS)}")
        print("[MQTT] Tekan Ctrl+C untuk berhenti.\n")
    else:
        print(f"[MQTT] Gagal terhubung, kode={rc}")


def on_publish(client, userdata, mid):
    pass  # silent — supaya output tidak berisik


# ─── Main Loop ────────────────────────────────────────────────────────────────
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_publish = on_publish

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_start()

        msg_count = 0
        station_idx = 0

        while True:
            t_start = time.perf_counter()

            station = STATIONS[station_idx % len(STATIONS)]
            payload = generate_payload(station)
            topic   = f"stasiun/cuaca/{payload['station_id']}"

            result = client.publish(topic, json.dumps(payload), qos=1)
            msg_count += 1
            station_idx += 1

            # Log ringkas setiap 25 pesan (5 detik sekali)
            if msg_count % 25 == 0:
                print(
                    f"[Publisher] #{msg_count:>6} pesan terkirim | "
                    f"terakhir: {payload['station_id']} {payload['lokasi']:<12} | "
                    f"suhu={payload['suhu_c']}°C  aqi={payload['aqi']}"
                )

            # Jaga interval 0.2 detik per pesan
            elapsed = time.perf_counter() - t_start
            sleep_time = INTERVAL - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print(f"\n[INFO] Publisher dihentikan. Total pesan terkirim: {msg_count}")
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        print(f"[ERROR] {e}")


if __name__ == "__main__":
    main()