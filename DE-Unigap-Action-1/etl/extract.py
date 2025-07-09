
# %%

import pandas as pd

def extract(csv_path):
    try:
        df = pd.read_csv(csv_path)
        print(f"📥 Đã trích xuất {len(df)} bản ghi từ {csv_path}")
        return df
    except Exception as e:
        print(f"❌ Lỗi khi trích xuất dữ liệu từ {csv_path}: {e}")
        raise