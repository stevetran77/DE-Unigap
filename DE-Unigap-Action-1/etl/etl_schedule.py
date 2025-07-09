import schedule
import time
from main import main

def run_job():
    print("🚀 Running ETL job...")
    main()

schedule.every(1).days.at_time("7:00").do(run_job)

while True:
    schedule.run_pending()
    time.sleep(2)