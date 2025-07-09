# main.py
#%%
from extract import extract
from transform import transform_data
from load import load_to_mysql


def main():
    csv_path = r"C:\Users\cau.tran\OneDrive\2. Study\13. Data Engineering with Unigap\Action 1\DE-Unigap-Action-1\data/data.csv"
    table_name = "it_salary"

    print("🚀 ETL pipeline bắt đầu...")

    # EXTRACT
    df_raw = extract(csv_path)

    # TRANSFORM
    df_clean = transform_data(df_raw)

    # LOAD
    load_to_mysql(df_clean, table_name)

    # print("✅ Pipeline hoàn tất!")

if __name__ == "__main__":
    main()
