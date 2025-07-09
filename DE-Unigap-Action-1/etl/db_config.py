# etl/db_config.py

import mysql.connector
from mysql.connector import Error

def get_connection():
    """
    Tạo kết nối đến MySQL database.
    Trả về: kết nối (connection object) nếu thành công, lỗi nếu không.
    """

    try:
        connection = mysql.connector.connect(
            host="localhost",         # hoặc IP nếu dùng remote DB
            user="root",              # thay bằng user của bạn
            password="******", # thay bằng mật khẩu thật
            database="unigap"  # tên database chứa bảng
        )
        if connection.is_connected():
            print("[INFO] Kết nối MySQL thành công")
            return connection

    except Error as e:
        print(f"[ERROR] Kết nối MySQL thất bại: {e}")
        return None
