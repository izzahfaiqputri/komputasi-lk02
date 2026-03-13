# Lembar Kerja Mahasiswa
## Praktikum Komputasi Terdistribusi
### Studi Kasus: Sistem Pemantauan Stasiun Cuaca Cerdas

**Mata Kuliah** : Sistem Komputasi Terdistribusi  
**Topik**       : Mekanisme Komputasi — MapReduce, Stream Processing, Parallel Processing  
**Waktu**       : 2 × 50 menit  
**Sifat**       : Individu

---

## Skenario

Sebuah kampus memiliki **5 stasiun cuaca cerdas** yang dipasang di berbagai titik
(Gedung A, Gedung B, Lapangan, Parkiran, Rooftop). Setiap stasiun mengirim data sensor
ke MQTT Broker secara real-time.

**Infrastruktur sudah disediakan:**

```
┌─────────────┐       ┌────────────────────────┐
│  Publisher  │──────►│  MQTT Broker           │
│  (berjalan) │       │  host : 10.34.100.103  │
└─────────────┘       │  port : 1883           │
                      │  topik: stasiun/cuaca/# │
                      └────────────────────────┘
```

**Tugas Anda:** Membangun tiga program subscriber yang masing-masing mengolah
data sensor menggunakan mekanisme komputasi yang berbeda.

---

## Skema Data

Setiap pesan MQTT berisi JSON dengan struktur berikut:

```json
{
  "timestamp":        "2026-03-09T08:00:01.234Z",
  "station_id":       "WS-001",
  "lokasi":           "Gedung_A",
  "lat":              -7.2504,
  "lon":              112.7688,
  "suhu_c":           31.5,
  "kelembaban_pct":   72.3,
  "tekanan_hpa":      1012.4,
  "kecepatan_angin":  14.2,
  "arah_angin":       "SE",
  "curah_hujan_mm":   0.0,
  "aqi":              87,
  "intensitas_lux":   45200
}
```

| Field | Satuan | Rentang Normal | Keterangan |
|-------|--------|----------------|------------|
| `suhu_c` | °C | 15 – 42 | Suhu udara |
| `kelembaban_pct` | % | 30 – 98 | Kelembaban relatif |
| `tekanan_hpa` | hPa | 995 – 1025 | Tekanan atmosfer |
| `kecepatan_angin` | km/h | 0 – 60 | Kecepatan angin |
| `arah_angin` | — | N/NE/E/SE/S/SW/W/NW | Arah angin |
| `curah_hujan_mm` | mm | 0 – 30 | Curah hujan per interval |
| `aqi` | — | 0 – 300 | Air Quality Index |
| `intensitas_lux` | lux | 0 – 100.000 | Intensitas cahaya |

**Kategori AQI:**

| Rentang | Kategori | Makna |
|---------|----------|-------|
| 0 – 50 | Baik | Aman |
| 51 – 100 | Sedang | Dapat diterima |
| 101 – 150 | Tidak Sehat (sensitif) | Bahaya bagi kelompok sensitif |
| 151 – 200 | Tidak Sehat | Berbahaya |
| 201 – 300 | Sangat Tidak Sehat | Darurat |

---

## Cara Menjalankan Publisher

Publisher sudah berjalan otomatis di infrastruktur yang disediakan.
Untuk pengujian lokal, jalankan:

```bash
# Install dependensi
pip install paho-mqtt

# Jalankan publisher
python publisher_cuaca.py
```

Publisher mengirim **5 pesan per detik** (satu dari setiap stasiun secara bergantian)
ke topik `stasiun/cuaca/<station_id>`.

Subscribe ke semua stasiun sekaligus menggunakan wildcard:

```python
client.subscribe("stasiun/cuaca/#", qos=1)
```

---

## Tugas Utama

Buat **tiga file Python** berikut di folder `subscriber/`:

```
subscriber/
├── solution_mapreduce.py     ← Tugas A
├── solution_stream.py        ← Tugas B
└── solution_parallel.py      ← Tugas C
```

Setiap file harus dapat dijalankan mandiri:

```bash
python solution_mapreduce.py
python solution_stream.py
python solution_parallel.py
```

---

## Tugas A — MapReduce

### Deskripsi

Implementasikan pipeline MapReduce yang mengolah data sensor secara **batch**.
Kumpulkan `N` pesan dari semua stasiun, lalu jalankan Map → Shuffle → Reduce.

### Ketentuan

**1. Konfigurasi (via environment variable atau konstanta):**

```python
BROKER_HOST = "10.34.100.103"
BROKER_PORT = 1883
TOPIC       = "stasiun/cuaca/#"
BATCH_SIZE  = 20   # jumlah pesan sebelum MapReduce dijalankan
```

**2. Fase MAP — wajib menghasilkan pasangan `(key, value)`:**

Key yang harus digunakan: `station_id`  
Value yang harus dipetakan:

```
(station_id, {
    "suhu_c"          : ...,
    "kelembaban_pct"  : ...,
    "aqi"             : ...,
    "curah_hujan_mm"  : ...,
    "kecepatan_angin" : ...
})
```

**3. Fase SHUFFLE — kelompokkan berdasarkan `station_id`**

**4. Fase REDUCE — untuk setiap stasiun hitung:**

```
- count         : jumlah data dalam batch
- suhu_avg      : rata-rata suhu
- suhu_max      : suhu tertinggi
- lembab_avg    : rata-rata kelembaban
- aqi_avg       : rata-rata AQI
- aqi_max       : AQI tertinggi dalam batch
- hujan_total   : total curah hujan (sum)
- status_udara  : kategori AQI rata-rata ("Baik"/"Sedang"/"Tidak Sehat"/...)
```

**5. Output yang diharapkan:**

```
════════════════════════════════════════════════════
[MapReduce] Batch #1  (20 record dari 5 stasiun)
────────────────────────────────────────────────────
[Map]     20 pasangan (station_id, nilai) terbentuk
[Shuffle] 5 grup: WS-001, WS-002, WS-003, WS-004, WS-005

[Reduce]  Hasil per stasiun:
  Stasiun  Count  Suhu Avg  Suhu Max  AQI Avg  AQI Max  Hujan  Status Udara
  ───────  ─────  ────────  ────────  ───────  ───────  ─────  ────────────
  WS-001       4     29.3C     31.2C       85      102   0.0mm  Sedang
  WS-002       4     28.7C     30.5C       92      115   2.3mm  Sedang
  ...
════════════════════════════════════════════════════
```

### Pertanyaan Laporan A

Setelah program berjalan, jawab pertanyaan berikut di laporan:

1. Berapa lama waktu yang dibutuhkan sebelum batch pertama selesai diproses? Mengapa membutuhkan waktu tersebut?

2. Apa yang terjadi dengan urutan data dari berbagai stasiun saat masuk ke buffer? Bagaimana fase Shuffle menangani hal ini?

3. Jika `BATCH_SIZE` ditingkatkan menjadi 100, apa dampaknya terhadap akurasi statistik dan latensi hasil?

4. **Modifikasi:** Ubah key pada fase Map menjadi `arah_angin` (bukan `station_id`).
   Apa insight baru yang bisa didapat dari perubahan ini?

---

## Tugas B — Stream Processing

### Deskripsi

Implementasikan stream processor yang memproses setiap event **langsung saat tiba**
dengan mekanisme windowing dan alerting real-time.

### Ketentuan

**1. Konfigurasi:**

```python
BROKER_HOST    = "10.34.100.103"
BROKER_PORT    = 1883
TOPIC          = "stasiun/cuaca/#"
WINDOW_SIZE    = 10    # jumlah event per stasiun untuk tumbling window
SLIDE_SIZE     = 5     # jumlah event per stasiun untuk sliding window

# Threshold alert
ALERT_SUHU_MAX     = 38.0    # °C
ALERT_AQI_MAX      = 150     # tidak sehat
ALERT_ANGIN_MAX    = 40.0    # km/h — waspada angin kencang
ALERT_HUJAN_MIN    = 5.0     # mm — waspada hujan
```

**2. Wajib implementasi:**

**a. Per-event processing** — setiap pesan diproses langsung:
- Tampilkan data ringkas per event
- Deteksi dan tampilkan alert jika ada kondisi ekstrem

**b. Sliding window per stasiun** — setiap stasiun punya window-nya sendiri:
- Simpan N event terakhir per `station_id`
- Setelah setiap event, hitung: `avg_suhu`, `avg_aqi`, `max_angin`

**c. Tumbling window per stasiun** — flush setiap `WINDOW_SIZE` event per stasiun:
- Hitung statistik lengkap dalam window
- Bandingkan rata-rata AQI stasiun satu sama lain
- Identifikasi stasiun dengan kondisi terburuk dalam window

**3. Struktur data per stasiun (gunakan dict of deque):**

```python
from collections import defaultdict, deque

_sliding  = defaultdict(lambda: deque(maxlen=SLIDE_SIZE))
_tumbling = defaultdict(list)
```

**4. Output yang diharapkan:**

```
[Stream #   1] WS-003 Lapangan  | suhu=30.2C  aqi= 88  angin=12.1km/h  hujan=0.0mm
[Stream #   2] WS-001 Gedung_A  | suhu=37.9C  aqi=155  angin=41.2km/h  hujan=6.2mm
               *** ALERT WS-001: SUHU TINGGI(37.9C) | AQI TIDAK SEHAT(155) | ANGIN KENCANG(41.2km/h) | HUJAN(6.2mm) ***
[Stream #   3] WS-002 Gedung_B  | suhu=28.5C  aqi= 72  angin= 8.3km/h  hujan=0.0mm
               [sliding WS-002 n=2] avg_suhu=28.5C  avg_aqi=72

...

  ┌── Tumbling Window WS-001 (10 event) ───────────────────
  │  suhu  : avg=33.1C  max=37.9C
  │  AQI   : avg=130  max=165  status=Tidak Sehat (sensitif)
  │  angin : avg=22.4km/h  max=41.2km/h
  │  hujan : total=8.4mm
  └─────────────────────────────────────────────────────────

  Peringkat stasiun (AQI avg, 10 event terakhir):
  1. WS-001 Gedung_A   : AQI avg=130  ← terburuk
  2. WS-004 Parkiran   : AQI avg=112
  ...
```

### Pertanyaan Laporan B

1. Mengapa `defaultdict(lambda: deque(maxlen=N))` lebih tepat digunakan
   dibanding satu deque global?

2. Dalam 10 menit berjalan, stasiun mana yang paling sering memicu alert?
   Apa interpretasinya secara kontekstual?

3. Apa perbedaan konkret antara **sliding** dan **tumbling** window
   yang Anda amati dari output program Anda?

4. **Modifikasi:** Tambahkan deteksi **tren kenaikan AQI** —
   jika AQI naik selama 3 event berturut-turut pada stasiun yang sama,
   tampilkan peringatan dini. Tuliskan implementasinya.

---

## Tugas C — Parallel Processing

### Deskripsi

Implementasikan subscriber yang mem-parallelkan komputasi menggunakan
`ThreadPoolExecutor`. Setiap event di-dispatch ke beberapa worker
yang berjalan **bersamaan**.

### Ketentuan

**1. Konfigurasi:**

```python
BROKER_HOST  = "10.34.100.103"
BROKER_PORT  = 1883
TOPIC        = "stasiun/cuaca/#"
NUM_WORKERS  = 4
REPORT_EVERY = 10   # cetak ringkasan global setiap N event
```

**2. Wajib implementasi minimal 4 worker:**

| Worker | Nama fungsi | Tugas |
|--------|-------------|-------|
| A | `worker_statistik_suhu` | Update running stats suhu per stasiun (min/max/avg) |
| B | `worker_kualitas_udara` | Klasifikasi AQI + hitung frekuensi tiap kategori |
| C | `worker_cuaca_ekstrem` | Deteksi kondisi ekstrem (angin kencang/hujan/suhu tinggi) |
| D | `worker_agregat_global` | Update agregat lintas stasiun (stasiun terpanas, AQI tertinggi) |

**3. Shared state yang harus dikelola (dengan `threading.Lock`):**

```python
# Statistik suhu per stasiun
_suhu_stats: dict[str, dict]  = defaultdict(lambda: {"n":0,"total":0.0,"min":99,"max":-99})

# Frekuensi kategori AQI
_aqi_kategori: dict[str, int] = {"Baik":0, "Sedang":0, "Tidak Sehat (sensitif)":0,
                                   "Tidak Sehat":0, "Sangat Tidak Sehat":0}

# Event ekstrem per stasiun
_ekstrem_count: dict[str, int] = defaultdict(int)

# Agregat global
_global = {"stasiun_terpanas": None, "stasiun_aqi_terburuk": None}
```

**4. Dispatch dan collect wajib menggunakan pola ini:**

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

_executor = ThreadPoolExecutor(max_workers=NUM_WORKERS)

# Di on_message:
futures = {_executor.submit(fn, payload): fn.__name__ for fn in WORKERS}
results = {name: f.result() for f, name in [(f, futures[f]) for f in as_completed(futures)]}
```

**5. Output ringkasan (setiap `REPORT_EVERY` event):**

```
  ┌── Ringkasan Global setelah 20 event ──────────────────────────
  │  STATISTIK SUHU PER STASIUN:
  │    WS-001 Gedung_A  : avg=30.2C  min=27.1C  max=36.8C  (n=4)
  │    WS-002 Gedung_B  : avg=29.7C  min=26.3C  max=33.2C  (n=4)
  │    ...
  │
  │  KUALITAS UDARA (distribusi kategori, total 20 event):
  │    Baik              :  8 event  (40%)
  │    Sedang            :  9 event  (45%)
  │    Tidak Sehat (sens):  3 event  (15%)
  │
  │  KONDISI EKSTREM PER STASIUN:
  │    WS-001 : 3 event ekstrem
  │    WS-003 : 1 event ekstrem
  │
  │  STASIUN TERPANAS  : WS-001 Gedung_A  (avg 30.2C)
  │  STASIUN AQI BURUK : WS-004 Parkiran  (AQI terakhir 138)
  └────────────────────────────────────────────────────────────────
```

### Pertanyaan Laporan C

1. Gambarkan **diagram urutan** (sequence diagram) yang menunjukkan
   bagaimana satu event diproses oleh 4 worker secara paralel,
   termasuk kapan `_lock` di-acquire dan di-release.

2. Apakah urutan hasil dari `as_completed()` selalu sama dengan
   urutan worker di-submit? Jelaskan mengapa dan apa implikasinya.

3. Jalankan program selama 5 menit. Catat `event ID` dari setiap
   ringkasan yang dicetak. Berapa rata-rata waktu antar ringkasan?
   Apakah sesuai dengan `REPORT_EVERY × interval_publisher`?

4. **Modifikasi:** Tambahkan pengukuran **waktu eksekusi aktual** setiap worker
   menggunakan `time.perf_counter()`. Tampilkan worker mana yang paling lambat.
   Worker mana yang paling diuntungkan dari parallelism?

---

## Ketentuan Umum Kode

Setiap file Python harus memenuhi syarat berikut:

- [ ] Terhubung ke broker `10.34.100.103:1883`
- [ ] Subscribe ke topik `stasiun/cuaca/#` (wildcard semua stasiun)
- [ ] Handle `json.JSONDecodeError` dan `KeyError` dengan try/except
- [ ] Dapat dihentikan dengan `Ctrl+C` tanpa error
- [ ] Menggunakan environment variable untuk konfigurasi:

```python
import os
BROKER_HOST = os.environ.get("MQTT_BROKER", "10.34.100.103")
BROKER_PORT = int(os.environ.get("MQTT_PORT", 1883))
```

---

## Kriteria Penilaian

### Tugas A — MapReduce (34 poin)

| Kriteria | Poin |
|----------|------|
| Buffer data & trigger batch berfungsi | 6 |
| Fase Map menghasilkan `(station_id, value)` | 8 |
| Fase Shuffle mengelompokkan per stasiun | 6 |
| Fase Reduce menghasilkan semua field yang diminta | 10 |
| Output terformat rapi & informatif | 4 |

### Tugas B — Stream Processing (33 poin)

| Kriteria | Poin |
|----------|------|
| Per-event processing & logging | 5 |
| Real-time alert untuk semua threshold | 8 |
| Sliding window per stasiun (defaultdict deque) | 8 |
| Tumbling window flush + peringkat antar stasiun | 8 |
| Output terformat rapi & informatif | 4 |

### Tugas C — Parallel Processing (33 poin)

| Kriteria | Poin |
|----------|------|
| 4 worker terimplementasi sesuai spesifikasi | 12 |
| Shared state dengan `_lock` (thread-safe) | 8 |
| Dispatch `as_completed()` berfungsi | 5 |
| Ringkasan global setiap `REPORT_EVERY` event | 4 |
| Output terformat rapi & informatif | 4 |

### Laporan & Analisis (+10 poin bonus)

| Kriteria | Poin Bonus |
|----------|------------|
| Semua pertanyaan laporan dijawab lengkap & tepat | 6 |
| Modifikasi salah satu tugas berhasil diimplementasi | 4 |

---

## Format Pengumpulan

```
NIM_NAMA/
├── solution_mapreduce.py
├── solution_stream.py
├── solution_parallel.py
└── laporan.md          ← jawaban semua pertanyaan A1-A4, B1-B4, C1-C4
```

Kumpulkan sebagai file `.zip` dengan nama `NIM_NAMA_praktikum_komputasi.zip`.

**Deadline:** sesuai instruksi dosen di kelas.

---

## Referensi

- [Paho MQTT Python Documentation](https://eclipse.dev/paho/clients/python/docs/)
- [Python `collections.deque`](https://docs.python.org/3/library/collections.html#collections.deque)
- [Python `concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html)
- `PENJELASAN_KOMPUTASI.md` — penjelasan lengkap ketiga mekanisme
- `subscriber_mapreduce.py`, `subscriber_stream.py`, `subscriber_parallel.py` — kode referensi

> **Catatan:** Kode referensi menggunakan data `sensor/suhu` (2 field).  
> Tugas ini menggunakan data `stasiun/cuaca` (10 field, 5 stasiun) —  
> lebih kompleks dan membutuhkan perancangan ulang, bukan sekadar menyalin.
