# load.py

import pandas as pd
import logging
from datetime import datetime
from db_config import get_connection

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def load_to_mysql(df, table_name="jobs_cleaned"):
    try:
        conn = get_connection()
        if conn is None:
            raise Exception("Không thể kết nối đến cơ sở dữ liệu MySQL")

        cursor = conn.cursor()

        # === STEP 1: Kiểm tra bảng gốc có tồn tại để backup ===
        cursor.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_name = '{table_name}'
        """)
        table_exists = cursor.fetchone()[0] > 0

        if table_exists:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_table = f"{table_name}_backup_{timestamp}"
            cursor.execute(f"""
                CREATE TABLE {backup_table} AS
                SELECT * FROM {table_name}
            """)
            logging.info(f"✅ Đã backup bảng `{table_name}` sang `{backup_table}`")
        else:
            logging.info(f"⚠️ Bảng `{table_name}` chưa tồn tại → bỏ qua backup.")

        # === STEP 2: Drop bảng gốc và tạo mới ===
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        cursor.execute(f"""
            CREATE TABLE {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                created_date VARCHAR(50),
                job_title VARCHAR(255),
                company VARCHAR(255),
                salary TEXT,
                address TEXT,
                time VARCHAR(50),
                link_description TEXT,
                min_salary FLOAT,
                max_salary FLOAT,
                unit_currency VARCHAR(10),
                city_1 VARCHAR(20),
                district_1 TEXT,
                city_2 VARCHAR(20),
                district_2 TEXT,
                city_3 VARCHAR(20),
                district_3 TEXT,
                city_4 VARCHAR(20),
                district_4 TEXT,
                job_group VARCHAR(20)
            )
        """)
        logging.info(f"✅ Đã tạo lại bảng `{table_name}`")

        # === STEP 3: Chuẩn bị dữ liệu insert ===
        expected_columns = [
            "created_date", "job_title", "company", "salary", "address", "time",
            "link_description", "min_salary", "max_salary", "unit_currency",
            "city_1", "district_1", "city_2", "district_2",
            "city_3", "district_3", "city_4", "district_4", "job_group"
        ]

        df_clean = df[expected_columns].copy()
        df_clean = df_clean.astype(object).where(pd.notna(df_clean), None)
        data = df_clean.values.tolist()

        # === STEP 4: Thực hiện bulk insert ===
        insert_query = f"""
            INSERT INTO {table_name} (
                {', '.join(expected_columns)}
            ) VALUES ({', '.join(['%s'] * len(expected_columns))})
        """

        cursor.executemany(insert_query, data)

        conn.commit()
        conn.close()
        logging.info(f"✅ Đã insert {len(data)} dòng vào bảng `{table_name}` thành công")

    except Exception as e:
        logging.error(f"❌ Lỗi khi bulk insert vào MySQL: {e}")

