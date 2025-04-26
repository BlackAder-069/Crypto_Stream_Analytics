import time
import pymysql
from datetime import datetime

try:
    # Use the same credentials as spark_stream_processor
    conn = pymysql.connect(
        host='localhost',
        user='coinuser',
        password='<Pass>',
        database='<LOCAL_DB>'
    )
    cursor = conn.cursor()

    print("\n=== Stream Processing Performance Analysis ===\n")
    
    # Check if stream_agg has data
    print("Checking stream_agg table status...")
    cursor.execute("SELECT COUNT(*) FROM stream_agg")
    total_records = cursor.fetchone()[0]
    
    if total_records == 0:
        print("[❌ WARNING]: stream_agg table is empty! Please ensure the Spark streaming job is running properly.")
    else:
        print(f"[✅ Total Records in stream_agg]: {total_records}")
        
        # Get the time range of data
        cursor.execute("SELECT MIN(start_time), MAX(end_time) FROM stream_agg")
        min_time, max_time = cursor.fetchone()
        print(f"[📅 Data Time Range]: {min_time} to {max_time}")
        
        # Get aggregation statistics
        start = time.time()
        cursor.execute("""
            SELECT 
                COUNT(*) as total_windows,
                AVG(update_count) as avg_updates_per_window,
                SUM(update_count) / TIMESTAMPDIFF(SECOND, MIN(start_time), MAX(end_time)) as throughput,
                COUNT(DISTINCT concat(asset_id_base,asset_id_quote)) as distinct_pairs
            FROM stream_agg
        """)
        stream_metrics = cursor.fetchone()
        stream_time = time.time() - start

        # Get raw data metrics for comparison
        start = time.time()
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT concat(asset_id_base,asset_id_quote)) as distinct_pairs
            FROM raw_coin_data
        """)
        raw_metrics = cursor.fetchone()
        raw_time = time.time() - start
        
        print("\n=== Stream vs Raw Data Comparison ===")
        print("\n[🔄 Stream Processing Metrics]")
        print(f"Processing Time: {stream_time:.3f} seconds")
        print(f"Total Windows: {stream_metrics[0]}")
        print(f"Avg Updates per Window: {stream_metrics[1]:.2f}")
        print(f"Processing Throughput: {stream_metrics[2]:.2f} updates/second")
        print(f"Distinct Currency Pairs: {stream_metrics[3]}")

        print("\n[📊 Raw Data Metrics]")
        print(f"Query Time: {raw_time:.3f} seconds")
        print(f"Total Records: {raw_metrics[0]}")
        print(f"Distinct Currency Pairs: {raw_metrics[1]}")

        # Calculate performance difference
        print("\n[📊 Performance Statistics]")
        time_diff_pct = ((raw_time - stream_time) / raw_time) * 100 if raw_time > 0 else 0
        print(f"Time Improvement: {time_diff_pct:.1f}% faster with streaming")

except pymysql.Error as e:
    print(f"[❌ Database Error]: {str(e)}")
except Exception as e:
    print(f"[❌ General Error]: {str(e)}")
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
    print("\n=== Analysis Complete ===\n")
