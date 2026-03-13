"""
solution_parallel.py
Tugas C — Parallel Processing
Praktikum Komputasi Terdistribusi
"""

import os
import json
import time
import threading
import paho.mqtt.client as mqtt
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Konfigurasi ─────────────────────────────────────────────────────────────
BROKER_HOST  = os.environ.get("MQTT_BROKER", "localhost")
BROKER_PORT  = int(os.environ.get("MQTT_PORT", 1883))
TOPIC        = "stasiun/cuaca/#"
NUM_WORKERS  = 4
REPORT_EVERY = 10

# ─── Shared State ─────────────────────────────────────────────────────────────
_lock = threading.Lock()

_suhu_stats: dict = defaultdict(lambda: {
    "n": 0, "total": 0.0, "min": 99.0, "max": -99.0, "lokasi": ""
})

_aqi_kategori_count: dict = {
    "Baik": 0,
    "Sedang": 0,
    "Tidak Sehat (sensitif)": 0,
    "Tidak Sehat": 0,
    "Sangat Tidak Sehat": 0,
}

_ekstrem_count: dict = defaultdict(int)

_global: dict = {
    "stasiun_terpanas":    None,
    "stasiun_aqi_terburuk": None,
    "aqi_terburuk_val":    -1,
    "suhu_terpanas_avg":   -99.0,
}

_aqi_terakhir: dict = {}   # {station_id: aqi_value}  untuk agregat global

_event_counter: int = 0
_worker_times: dict = defaultdict(list)  # {worker_name: [elapsed_ms, ...]}

# ─── Helpers ─────────────────────────────────────────────────────────────────
def aqi_kategori(aqi: float) -> str:
    if aqi <= 50:   return "Baik"
    if aqi <= 100:  return "Sedang"
    if aqi <= 150:  return "Tidak Sehat (sensitif)"
    if aqi <= 200:  return "Tidak Sehat"
    return "Sangat Tidak Sehat"


# ─── Workers ─────────────────────────────────────────────────────────────────
def worker_statistik_suhu(payload: dict) -> str:
    t0  = time.perf_counter()
    sid = payload["station_id"]
    s   = payload["suhu_c"]
    lok = payload.get("lokasi", "")

    with _lock:
        st = _suhu_stats[sid]
        st["n"]     += 1
        st["total"] += s
        st["lokasi"]  = lok
        if s < st["min"]: st["min"] = s
        if s > st["max"]: st["max"] = s

    elapsed = (time.perf_counter() - t0) * 1000
    with _lock:
        _worker_times["worker_statistik_suhu"].append(elapsed)

    return f"suhu {sid}: avg={(st['total']/st['n']):.1f}°C"


def worker_kualitas_udara(payload: dict) -> str:
    t0  = time.perf_counter()
    aqi = payload["aqi"]
    kat = aqi_kategori(aqi)

    with _lock:
        _aqi_kategori_count[kat] += 1

    elapsed = (time.perf_counter() - t0) * 1000
    with _lock:
        _worker_times["worker_kualitas_udara"].append(elapsed)

    return f"AQI {aqi} → {kat}"


def worker_cuaca_ekstrem(payload: dict) -> str:
    t0  = time.perf_counter()
    sid = payload["station_id"]
    ekstrem = []

    if payload["suhu_c"]          >= 38.0: ekstrem.append("suhu_tinggi")
    if payload["aqi"]             >= 150:  ekstrem.append("aqi_tidak_sehat")
    if payload["kecepatan_angin"] >= 40.0: ekstrem.append("angin_kencang")
    if payload["curah_hujan_mm"]  >= 5.0:  ekstrem.append("hujan_lebat")

    if ekstrem:
        with _lock:
            _ekstrem_count[sid] += 1

    elapsed = (time.perf_counter() - t0) * 1000
    with _lock:
        _worker_times["worker_cuaca_ekstrem"].append(elapsed)

    return f"ekstrem {sid}: {ekstrem if ekstrem else 'none'}"


def worker_agregat_global(payload: dict) -> str:
    t0  = time.perf_counter()
    sid = payload["station_id"]
    aqi = payload["aqi"]
    lok = payload.get("lokasi", "")

    with _lock:
        _aqi_terakhir[sid] = {"aqi": aqi, "lokasi": lok}

        # Tentukan stasiun AQI terburuk berdasarkan nilai terbaru
        worst_sid  = max(_aqi_terakhir, key=lambda k: _aqi_terakhir[k]["aqi"])
        worst_val  = _aqi_terakhir[worst_sid]["aqi"]
        worst_lok  = _aqi_terakhir[worst_sid]["lokasi"]
        _global["stasiun_aqi_terburuk"] = f"{worst_sid} {worst_lok}"
        _global["aqi_terburuk_val"]     = worst_val

        # Stasiun terpanas berdasarkan rata-rata suhu saat ini
        if _suhu_stats:
            hot_sid = max(
                _suhu_stats,
                key=lambda k: _suhu_stats[k]["total"] / max(_suhu_stats[k]["n"], 1)
            )
            hot_avg = _suhu_stats[hot_sid]["total"] / max(_suhu_stats[hot_sid]["n"], 1)
            hot_lok = _suhu_stats[hot_sid]["lokasi"]
            _global["stasiun_terpanas"]      = f"{hot_sid} {hot_lok}"
            _global["suhu_terpanas_avg"]     = hot_avg

    elapsed = (time.perf_counter() - t0) * 1000
    with _lock:
        _worker_times["worker_agregat_global"].append(elapsed)

    return f"global updated"


# Daftar worker yang akan di-dispatch
WORKERS = [
    worker_statistik_suhu,
    worker_kualitas_udara,
    worker_cuaca_ekstrem,
    worker_agregat_global,
]

# ─── Thread Pool ──────────────────────────────────────────────────────────────
_executor = ThreadPoolExecutor(max_workers=NUM_WORKERS)


# ─── Ringkasan Global ─────────────────────────────────────────────────────────
def print_ringkasan(n_event: int):
    with _lock:
        suhu_snap    = dict(_suhu_stats)
        aqi_snap     = dict(_aqi_kategori_count)
        ekst_snap    = dict(_ekstrem_count)
        glob_snap    = dict(_global)
        total_aqi    = sum(aqi_snap.values())
        wtimes_snap  = {k: list(v) for k, v in _worker_times.items()}

    print()
    print(f"  ┌── Ringkasan Global setelah {n_event} event " + "─" * 30)
    print(f"  │")
    print(f"  │  STATISTIK SUHU PER STASIUN:")
    for sid in sorted(suhu_snap):
        st  = suhu_snap[sid]
        n   = st["n"]
        avg = st["total"] / n if n else 0
        print(
            f"  │    {sid} {st['lokasi']:<12}: "
            f"avg={avg:.1f}°C  min={st['min']:.1f}°C  max={st['max']:.1f}°C  (n={n})"
        )
    print(f"  │")
    print(f"  │  KUALITAS UDARA (distribusi kategori, total {total_aqi} event):")
    for kat, cnt in aqi_snap.items():
        pct = (cnt / total_aqi * 100) if total_aqi else 0
        print(f"  │    {kat:<26}: {cnt:>3} event  ({pct:.0f}%)")
    print(f"  │")
    print(f"  │  KONDISI EKSTREM PER STASIUN:")
    if ekst_snap:
        for sid, cnt in sorted(ekst_snap.items(), key=lambda x: x[1], reverse=True):
            print(f"  │    {sid} : {cnt} event ekstrem")
    else:
        print(f"  │    (belum ada)")
    print(f"  │")
    print(f"  │  STASIUN TERPANAS  : {glob_snap.get('stasiun_terpanas','?')}  "
          f"(avg {glob_snap.get('suhu_terpanas_avg', 0):.1f}°C)")
    print(f"  │  STASIUN AQI BURUK : {glob_snap.get('stasiun_aqi_terburuk','?')}  "
          f"(AQI terakhir {glob_snap.get('aqi_terburuk_val',0)})")
    print(f"  │")
    print(f"  │  WAKTU EKSEKUSI WORKER (rata-rata ms):")
    for wname, times in sorted(wtimes_snap.items()):
        if times:
            avg_t = sum(times) / len(times)
            max_t = max(times)
            print(f"  │    {wname:<32}: avg={avg_t:.4f}ms  max={max_t:.4f}ms")
    # Identifikasi worker paling lambat
    if wtimes_snap:
        slowest = max(wtimes_snap, key=lambda k: sum(wtimes_snap[k]) / max(len(wtimes_snap[k]),1))
        print(f"  │  ⚠ Worker PALING LAMBAT: {slowest}")
    print(f"  └" + "─" * 60)


# ─── MQTT Callbacks ──────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Terhubung ke broker {BROKER_HOST}:{BROKER_PORT}")
        client.subscribe(TOPIC, qos=1)
        print(f"[MQTT] Subscribe ke topik '{TOPIC}'  |  NUM_WORKERS={NUM_WORKERS}  REPORT_EVERY={REPORT_EVERY}")
        print("[MQTT] Menunggu pesan ...\n")
    else:
        print(f"[MQTT] Gagal terhubung, kode={rc}")


def on_message(client, userdata, msg):
    global _event_counter
    try:
        payload = json.loads(msg.payload.decode())

        with _lock:
            _event_counter += 1
            current_id = _event_counter

        # ── Dispatch ke semua worker secara paralel ────────────────────
        futures = {
            _executor.submit(fn, payload): fn.__name__
            for fn in WORKERS
        }
        results = {
            futures[f]: f.result()
            for f in as_completed(futures)
        }

        sid  = payload["station_id"]
        lok  = payload.get("lokasi", "")
        print(
            f"[Parallel #{current_id:>4}] {sid} {lok:<12} | "
            + "  ".join(f"{k}={v}" for k, v in results.items())
        )

        # ── Ringkasan periodik ─────────────────────────────────────────
        if current_id % REPORT_EVERY == 0:
            print_ringkasan(current_id)

    except json.JSONDecodeError as e:
        print(f"[ERROR] JSONDecodeError: {e}")
    except KeyError as e:
        print(f"[ERROR] KeyError — field tidak ditemukan: {e}")


# ─── Entry Point ──────────────────────────────────────────────────────────────
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Dihentikan oleh pengguna (Ctrl+C).")
        with _lock:
            final_count = _event_counter
        if final_count:
            print_ringkasan(final_count)
        _executor.shutdown(wait=False)
        client.disconnect()
    except Exception as e:
        print(f"[ERROR] Tidak bisa terhubung ke broker: {e}")


if __name__ == "__main__":
    main()