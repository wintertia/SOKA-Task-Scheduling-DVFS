# SOKA Task Scheduling (DVFS Algorithm)

## Kelas B Kelompok B

Nama | NRP
---|---
Muhammad Faqih Husain | 5027231023
Kevin Anugerah Faza | 5027231027
Amoes Noland | 5027231028
Rafi' Afnaan Fathurrahman | 5027231040
Azza Farichi Tjahjono | 5027231071

# Pengujian Algoritma Task Scheduler pada Server IT

Repo ini merupakan kode dari server yang digunakan dalam pengujian Task Scheduling pada Server IT serta contoh algoritma scheduler untuk keperluan mata kuliah **Strategi Optimasi Komputasi Awan (SOKA)**

## Cara Penggunaan - Dev

1. Install `uv` sebagai dependency manager. Lihat [link berikut](https://docs.astral.sh/uv/getting-started/installation/)

2. Install semua requirement

```bash
uv sync
```

3. Buat file `.env` kemudian isi menggunakan variabel pada `.env.example`. Isi nilai setiap variabel sesuai kebutuhan

```conf
VM1_IP=""
VM2_IP=""
VM3_IP=""
VM4_IP=""

VM_PORT=5000
```

4. Algoritma pada contoh di sini merupakan algoritma `Stochastic Hill Climbing`.

![shc_algorithm](https://i.sstatic.net/HISbC.png)

5. Untuk menjalankan server, jalankan docker

```bash
docker compose build --no-cache
docker compose up -d
```

6. Inisiasi Dataset untuk scheduler. Buat file `dataset.txt` kemudian isi dengan dataset berupa angka 1 - 10. Berikut adalah contohnya:

```txt
6
5
8
2
10
3
4
4
7
3
9
1
7
9
1
8
2
5
6
10
```

7. Untuk menjalankan scheduler, jalankan file `scheduler.py`. **Jangan lupa menggunakan VPN / Wifi ITS**

```bash
uv run scheduler.py
```

8. Apabila sukses, akan terdapat hasil berupa file `result.csv` dan pada console akan tampil perhitungan parameter untuk kebutuhan analisis.

`result.csv`

![result csv](./images/result-csv.png)

`console`

![console](./images/console.png)