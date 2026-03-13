"""
solution_mapreduce.py
Tugas A — MapReduce
Praktikum Komputasi Terdistribusi
"""

import os
import json
import time
import paho.mqtt.client as mqtt

# ─── Konfigurasi ────────────────────────────────────────────────────────────
BROKER_HOST = os.environ.get("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.environ.get("MQTT_PORT", 1883))
TOPIC       = "stasiun/cuaca/#"
BATCH_SIZE  = 20
# BATCH_SIZE  = 100 #Tugas A1 

# ─── Kategori AQI ───────────────────────────────────────────────────────────
def aqi_kategori(aqi: float) -> str:
    if aqi <= 50:   return "Baik"
    if aqi <= 100:  return "Sedang"
    if aqi <= 150:  return "Tidak Sehat (sensitif)"
    if aqi <= 200:  return "Tidak Sehat"
    return "Sangat Tidak Sehat"

# ─── Buffer ──────────────────────────────────────────────────────────────────
_buffer: list[dict] = []
_batch_count: int   = 0

# ─── MapReduce Pipeline ──────────────────────────────────────────────────────
def phase_map(records: list[dict]) -> list[tuple]:
    """Map: setiap record → (station_id, value_dict)"""
    pairs = []
    for r in records:
        key   = r["station_id"]
        value = {
            "suhu_c":           r["suhu_c"],
            "kelembaban_pct":   r["kelembaban_pct"],
            "aqi":              r["aqi"],
            "curah_hujan_mm":   r["curah_hujan_mm"],
            "kecepatan_angin":  r["kecepatan_angin"],
        }
        pairs.append((key, value))
    return pairs


def phase_shuffle(pairs: list[tuple]) -> dict[str, list]:
    """Shuffle: kelompokkan list value berdasarkan station_id"""
    grouped: dict[str, list] = {}
    for key, value in pairs:
        grouped.setdefault(key, []).append(value)
    return grouped


def phase_reduce(grouped: dict[str, list]) -> list[dict]:
    """Reduce: hitung statistik aggregat per stasiun"""
    results = []
    for station_id, values in sorted(grouped.items()):
        n         = len(values)
        suhu_list = [v["suhu_c"]          for v in values]
        lmbd_list = [v["kelembaban_pct"]   for v in values]
        aqi_list  = [v["aqi"]              for v in values]
        hujan_lst = [v["curah_hujan_mm"]   for v in values]

        aqi_avg_val = sum(aqi_list) / n

        results.append({
            "station_id":  station_id,
            "count":       n,
            "suhu_avg":    sum(suhu_list) / n,
            "suhu_max":    max(suhu_list),
            "lembab_avg":  sum(lmbd_list) / n,
            "aqi_avg":     aqi_avg_val,
            "aqi_max":     max(aqi_list),
            "hujan_total": sum(hujan_lst),
            "status_udara": aqi_kategori(aqi_avg_val),
        })
    return results


def run_mapreduce(records: list[dict], batch_num: int):
    t0 = time.perf_counter()

    # MAP
    pairs   = phase_map(records)
    # SHUFFLE
    grouped = phase_shuffle(pairs)
    # REDUCE
    results = phase_reduce(grouped)

    elapsed = (time.perf_counter() - t0) * 1000  # ms

    # ─── Output ──────────────────────────────────────────────────────────
    stasiun_list = ", ".join(sorted(grouped.keys()))
    print()
    print("═" * 60)
    print(f"[MapReduce] Batch #{batch_num}  ({len(records)} record dari {len(grouped)} stasiun)")
    print("─" * 60)
    print(f"[Map]     {len(pairs)} pasangan (station_id, nilai) terbentuk")
    print(f"[Shuffle] {len(grouped)} grup: {stasiun_list}")
    print()
    print("[Reduce]  Hasil per stasiun:")

    header = (
        f"  {'Stasiun':<8}  {'Count':>5}  {'Suhu Avg':>8}  {'Suhu Max':>8}  "
        f"{'AQI Avg':>7}  {'AQI Max':>7}  {'Hujan':>7}  {'Status Udara'}"
    )
    sep = (
        f"  {'───────':<8}  {'─────':>5}  {'────────':>8}  {'────────':>8}  "
        f"{'───────':>7}  {'───────':>7}  {'─────':>7}  {'────────────'}"
    )
    print(header)
    print(sep)

    for r in results:
        print(
            f"  {r['station_id']:<8}  {r['count']:>5}  "
            f"  {r['suhu_avg']:>5.1f}°C  {r['suhu_max']:>5.1f}°C  "
            f"{r['aqi_avg']:>7.1f}  {r['aqi_max']:>7}  "
            f"{r['hujan_total']:>5.1f}mm  {r['status_udara']}"
        )

    print(f"\n  ⏱  Waktu proses MapReduce: {elapsed:.2f} ms")
    print("═" * 60)


# ─── MQTT Callbacks ──────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Terhubung ke broker {BROKER_HOST}:{BROKER_PORT}")
        client.subscribe(TOPIC, qos=1)
        print(f"[MQTT] Subscribe ke topik '{TOPIC}'  |  BATCH_SIZE={BATCH_SIZE}")
        print("[MQTT] Menunggu pesan ...\n")
    else:
        print(f"[MQTT] Gagal terhubung, kode={rc}")


def on_message(client, userdata, msg):
    global _buffer, _batch_count
    try:
        payload = json.loads(msg.payload.decode())
        _buffer.append(payload)

        if len(_buffer) >= BATCH_SIZE:
            _batch_count += 1
            batch   = _buffer[:BATCH_SIZE]
            _buffer = _buffer[BATCH_SIZE:]
            run_mapreduce(batch, _batch_count)

    except json.JSONDecodeError as e:
        print(f"[ERROR] JSONDecodeError: {e}")
    except KeyError as e:
        print(f"[ERROR] KeyError — field tidak ditemukan: {e}")


# ─── Entry Point ─────────────────────────────────────────────────────────────
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Dihentikan oleh pengguna (Ctrl+C).")
        client.disconnect()
    except Exception as e:
        print(f"[ERROR] Tidak bisa terhubung ke broker: {e}")


if __name__ == "__main__":
    main()