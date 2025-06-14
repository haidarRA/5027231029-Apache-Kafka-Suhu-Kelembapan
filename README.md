# Problem Based Learning : Apache Kafka

Tugas Big Data B Apache Kafka

| Nama Lengkap           | NRP           |
| :--------------------: | :-----------: |
| Haidar Rafi Aqyla      | 5027231029    |

# Langkah Pengerjaan & Dokumentasi
1. Install Spark dan Apache Kafka terlebih dahulu.
2. Jalankan Zookeeper dan Kafka dengan command `.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties` dan `.\bin\windows\kafka-server-start.bat .\config\server.properties`.
![image](https://github.com/user-attachments/assets/ca7edf83-c5de-4419-bfdb-7ac9b4d0e3a6)
![image](https://github.com/user-attachments/assets/cd9bee24-9b46-458c-ad0a-a38c5f3c7861)
3. Buat topic baru untuk kafka dengan nama `sensor-suhu-gudang` dan `sensor-kelembapan-gudang` menggunakan command `.\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic sensor-suhu-gudang` dan `.\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --topic sensor-kelembapan-gudang`
4. Jalankan kedua producer terlebih dahulu.
![image](https://github.com/user-attachments/assets/be6bce30-7227-4b4a-8f68-3dcb9a325c51)
![image](https://github.com/user-attachments/assets/b02c21cf-9013-49c2-acb9-573a4f0dbada)
5. Setelah itu, jalankan consumer PySpark dengan command `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_consumer_script.py`.

   ![big data suhu](https://github.com/user-attachments/assets/6687350e-e64c-4e6f-a4af-cdd1a0224c3c)
   ![big data kelembapan](https://github.com/user-attachments/assets/12eb8632-14a4-4818-a21f-8efc0186d0a0)
   ![image](https://github.com/user-attachments/assets/c9e3b74c-631e-4fd0-afb3-ee1b01305432)

Pada consumer, terdapat kolom warning yang bekerja sesuai dengan syarat soal. Jika suhu > 80, maka ada warning "Peringatan Suhu Tinggi". Begitu juga dengan kelembapan > 70, akan ada warning "Peringatan Kelembapan Tinggi". Untuk join multi-stream pun sama, dengan syarat berikut.
1. suhu > 80 dan kelembapan <= 70: "Suhu tinggi, kelembapan normal" 
2. suhu <= 80 dan kelembapan > 70: "Kelembapan tinggi, suhu aman"
3. suhu > 80 dan kelembapan > 70: "Bahaya tinggi! Barang berisiko rusak"
4. suhu <= 80 dan kelembapan <= 70: "Aman"
