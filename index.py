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
    # LANGKAH 1: EXTRACT - Mengambil Data dari API
    # =================================================================
    api_url = "https://jsonplaceholder.typicode.com/users"
    print(f"Mengambil data dari {api_url}...")
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Akan error jika status code bukan 200
        data = response.json()
        print("Data berhasil diambil.")
    except requests.exceptions.RequestException as e:
        print(f"Gagal mengambil data dari API: {e}")
        return

    # =================================================================
    # LANGKAH 2: TRANSFORM - Membersihkan Data dengan Pandas
    # =================================================================
    print("Memproses dan membersihkan data dengan pandas...")
    
    # Membuat DataFrame dari data JSON
    df = pd.DataFrame(data)

    # Memilih hanya kolom yang kita butuhkan
    kolom_pilihan = ['id', 'name', 'username', 'email', 'phone', 'website']
    df_clean = df[kolom_pilihan].copy() # Menggunakan .copy() untuk menghindari SettingWithCopyWarning

    # Mengambil data 'city' dari kolom 'address' yang merupakan JSON nested
    # Menggunakan .get() untuk keamanan jika 'city' tidak ada
    df_clean['city'] = df['address'].apply(lambda addr: addr.get('city', None))
    
    # Mengubah nama kolom agar lebih sesuai untuk database
    df_clean.rename(columns={'id': 'user_id'}, inplace=True)
    
    print("Data berhasil dibersihkan. Pratinjau data:")
    print(df_clean.head())
    
    # =================================================================
    # LANGKAH 3: LOAD - Memasukkan Data ke Database SQLite
    # =================================================================
    db_file = 'users.db'
    table_name = 'registered_users'
    
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
    db_file = 'users.db'
    table_name = 'registered_users'
    
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