import requests
import pandas as pd
import sqlite3
import os

def proses_data_pengguna():
    """
    Fungsi untuk mengambil data pengguna dari JSONPlaceholder,
    membersihkannya dengan pandas, dan memuatnya ke database SQLite.
    """
    # =================================================================
    # LANGKAH 1: EXTRACT - Mengambil Data dari CSV
    # =================================================================
    file_csv = "data/update.csv" # Ganti dengan nama file CSV Anda
    print(f"Mengambil data dari {file_csv}...")

    try:
        df = pd.read_csv(file_csv) # Data langsung menjadi DataFrame
        print("Data CSV berhasil dibaca.")
    except FileNotFoundError:
        print(f"Error: File '{file_csv}' tidak ditemukan.")
        # Keluar dari skrip jika file tidak ada
        exit()

    # =================================================================
    # LANGKAH 2: TRANSFORM - Membersihkan Data dengan Pandas
    # =================================================================
    print("Memproses dan membersihkan data dengan pandas...")

    # Pastikan nama kolom di CSV Anda sesuai dengan yang diharapkan
    kolom_pilihan = ['id','title','vote_average','vote_count']

    # Cek apakah kolom yang dibutuhkan ada di CSV
    kolom_tersedia = [kol for kol in kolom_pilihan if kol in df.columns]
    if len(kolom_tersedia) < len(kolom_pilihan):
        print(f"Peringatan: Beberapa kolom tidak ditemukan di CSV. Hanya akan memproses: {kolom_tersedia}")

    df_clean = df[kolom_pilihan].copy() # Menggunakan .copy() untuk menghindari SettingWithCopyWarning

    # Mengambil data 'city' dari kolom 'address' yang merupakan JSON nested
    # Menggunakan .get() untuk keamanan jika 'city' tidak ada
    # df_clean['city'] = df['address'].apply(lambda addr: addr.get('city', None))
    
    # Mengubah nama kolom agar lebih sesuai untuk database
    # df_clean.rename(columns={'id': 'user_id'}, inplace=True)
    
    print("Data berhasil dibersihkan. Pratinjau data:")
    print(df_clean.head())
    
    # =================================================================
    # LANGKAH 3: LOAD - Memasukkan Data ke Database SQLite
    # =================================================================
    db_file = 'movies.db'
    table_name = 'vote_movies'
    
    print(f"Menyimpan data ke database SQLite '{db_file}' di tabel '{table_name}'...")

    # Menghapus file database lama jika ada, untuk memastikan proses berjalan dari awal
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"File database '{db_file}' yang lama telah dihapus.")

    # Membuat koneksi ke database SQLite
    conn = sqlite3.connect(db_file)
    
    # Menggunakan metode to_sql() dari pandas untuk memasukkan data
    # if_exists='replace': akan membuat tabel baru dan menimpa jika sudah ada
    # index=False: agar index dari DataFrame tidak ikut masuk sebagai kolom di tabel
    df_clean.to_sql(table_name, conn, if_exists='replace', index=False)
    
    # Menutup koneksi
    conn.close()
    
    print("Data berhasil dimasukkan ke database SQLite.")


def verifikasi_data_db():
    """
    Fungsi opsional untuk membaca dan menampilkan data dari SQLite
    untuk memverifikasi bahwa data telah berhasil disimpan.
    """
    db_file = 'movies.db'
    table_name = 'vote_movies'
    
    if not os.path.exists(db_file):
        print(f"Database '{db_file}' tidak ditemukan. Jalankan proses utama terlebih dahulu.")
        return

    print("\n--- Verifikasi Data di Database ---")
    conn = sqlite3.connect(db_file)
    
    # Membaca data dari tabel SQLite ke dalam DataFrame baru
    df_from_db = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    
    print(f"Berhasil membaca data dari tabel '{table_name}':")
    print(df_from_db)
    
    conn.close()


# Menjalankan fungsi utama
if __name__ == "__main__":
    proses_data_pengguna()
    verifikasi_data_db()