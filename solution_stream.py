import os
import json
import paho.mqtt.client as mqtt
from collections import defaultdict, deque

# Konfigurasi 
BROKER_HOST = os.environ.get("MQTT_BROKER", "localhost")
BROKER_PORT = int(os.environ.get("MQTT_PORT", 1883))
TOPIC       = "stasiun/cuaca/#"

WINDOW_SIZE = 10   
SLIDE_SIZE  = 5    

ALERT_SUHU_MAX  = 38.0
ALERT_AQI_MAX   = 150
ALERT_ANGIN_MAX = 40.0
ALERT_HUJAN_MIN = 5.0

# State
_event_counter: int = 0

_sliding: dict  = defaultdict(lambda: deque(maxlen=SLIDE_SIZE))

_tumbling: dict = defaultdict(list)

_alert_count: dict = defaultdict(int)

# Helpers
def aqi_kategori(aqi: float) -> str:
    if aqi <= 50:   return "Baik"
    if aqi <= 100:  return "Sedang"
    if aqi <= 150:  return "Tidak Sehat (sensitif)"
    if aqi <= 200:  return "Tidak Sehat"
    return "Sangat Tidak Sehat"


def check_alerts(p: dict) -> list[str]:
    """Kembalikan list string kondisi yang melampaui threshold."""
    alerts = []
    if p["suhu_c"]          >= ALERT_SUHU_MAX:  alerts.append(f"SUHU TINGGI({p['suhu_c']}°C)")
    if p["aqi"]             >= ALERT_AQI_MAX:   alerts.append(f"AQI TIDAK SEHAT({p['aqi']})")
    if p["kecepatan_angin"] >= ALERT_ANGIN_MAX:  alerts.append(f"ANGIN KENCANG({p['kecepatan_angin']}km/h)")
    if p["curah_hujan_mm"]  >= ALERT_HUJAN_MIN:  alerts.append(f"HUJAN({p['curah_hujan_mm']}mm)")
    return alerts


# Tumbling Window flush 
def flush_tumbling(station_id: str, events: list[dict]):
    n           = len(events)
    suhu_list   = [e["suhu_c"]          for e in events]
    aqi_list    = [e["aqi"]              for e in events]
    angin_list  = [e["kecepatan_angin"]  for e in events]
    hujan_total = sum(e["curah_hujan_mm"] for e in events)
    aqi_avg     = sum(aqi_list) / n

    lokasi = events[-1].get("lokasi", "")

    print()
    print(f"  ┌── Tumbling Window {station_id} {lokasi} ({n} event) " + "─" * 20)
    print(f"  │  suhu  : avg={sum(suhu_list)/n:.1f}°C  max={max(suhu_list):.1f}°C")
    print(f"  │  AQI   : avg={aqi_avg:.0f}  max={max(aqi_list)}  status={aqi_kategori(aqi_avg)}")
    print(f"  │  angin : avg={sum(angin_list)/n:.1f}km/h  max={max(angin_list):.1f}km/h")
    print(f"  │  hujan : total={hujan_total:.1f}mm")
    print(f"  └" + "─" * 55)


def print_tumbling_ranking():
    """Cetak peringkat stasiun berdasarkan AQI rata-rata dari tumbling window yang sudah ada."""
    ranking = []
    for sid, events in _tumbling.items():
        if events:
            avg = sum(e["aqi"] for e in events) / len(events)
            lokasi = events[-1].get("lokasi", "")
            ranking.append((sid, lokasi, avg))
    if not ranking:
        return
    ranking.sort(key=lambda x: x[2], reverse=True)
    print()
    print(f"  Peringkat stasiun (AQI avg, window terakhir):")
    for i, (sid, lok, avg) in enumerate(ranking, 1):
        marker = "  ← terburuk" if i == 1 else ""
        print(f"  {i}. {sid} {lok:<12}: AQI avg={avg:.0f}{marker}")


# Pre-event Processing 
def process_event(payload: dict):
    global _event_counter
    _event_counter += 1

    sid    = payload["station_id"]
    lokasi = payload.get("lokasi", "")
    suhu   = payload["suhu_c"]
    aqi    = payload["aqi"]
    angin  = payload["kecepatan_angin"]
    hujan  = payload["curah_hujan_mm"]

    # Pre-event log 
    print(
        f"[Stream #{_event_counter:>4}] {sid} {lokasi:<12} | "
        f"suhu={suhu:.1f}°C  aqi={aqi:>3}  angin={angin:.1f}km/h  hujan={hujan:.1f}mm"
    )

    # Alert
    alerts = check_alerts(payload)
    if alerts:
        _alert_count[sid] += 1
        alert_str = " | ".join(alerts)
        print(f"               *** ALERT {sid}: {alert_str} ***")

    #  Sliding window
    _sliding[sid].append(payload)
    sw = _sliding[sid]
    n  = len(sw)
    if n >= 2:
        avg_suhu = sum(e["suhu_c"] for e in sw) / n
        avg_aqi  = sum(e["aqi"]    for e in sw) / n
        max_angin = max(e["kecepatan_angin"] for e in sw)
        print(
            f"               [sliding {sid} n={n}] "
            f"avg_suhu={avg_suhu:.1f}°C  avg_aqi={avg_aqi:.0f}  max_angin={max_angin:.1f}km/h"
        )

    # Tugas B4
    sw_list = list(sw)
    if len(sw_list) >= 3:
        last3 = [e["aqi"] for e in sw_list[-3:]]
        if last3[0] < last3[1] < last3[2]:
            print(
                f"               ⚠ TREN NAIK AQI {sid}: "
                f"{last3[0]} → {last3[1]} → {last3[2]} (3 event berurutan)"
            )

    #  Tumbling window
    _tumbling[sid].append(payload)
    if len(_tumbling[sid]) >= WINDOW_SIZE:
        flush_tumbling(sid, _tumbling[sid])
        print_tumbling_ranking()
        _tumbling[sid] = []   # reset


#  MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"[MQTT] Terhubung ke broker {BROKER_HOST}:{BROKER_PORT}")
        client.subscribe(TOPIC, qos=1)
        print(f"[MQTT] Subscribe ke topik '{TOPIC}'")
        print(f"[MQTT] WINDOW_SIZE={WINDOW_SIZE}  SLIDE_SIZE={SLIDE_SIZE}")
        print("[MQTT] Menunggu pesan ...\n")
    else:
        print(f"[MQTT] Gagal terhubung, kode={rc}")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        process_event(payload)
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSONDecodeError: {e}")
    except KeyError as e:
        print(f"[ERROR] KeyError — field tidak ditemukan: {e}")


#  Entry Point
def main():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Dihentikan oleh pengguna (Ctrl+C).")
        # Cetak ringkasan alert saat keluar
        if _alert_count:
            print("\n[SUMMARY] Total alert per stasiun:")
            for sid, cnt in sorted(_alert_count.items(), key=lambda x: x[1], reverse=True):
                print(f"  {sid}: {cnt} alert")
        client.disconnect()
    except Exception as e:
        print(f"[ERROR] Tidak bisa terhubung ke broker: {e}")


if __name__ == "__main__":
    main()