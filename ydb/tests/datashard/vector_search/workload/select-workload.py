from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import time
import os
import yaml
import ydb
import numpy as np

# === Configuration ===
CONFIG_PATH = os.path.expanduser("~/.ydb/config/config.yaml")
DURATION = 10  # seconds
MAX_WORKERS = 8
TX_MODE = ydb.QuerySnapshotReadOnly()

TABLE = "wikipedia"
INDEX = "idx_vector"
EMBEDDING_DIM = 768
K = 5

QUERY = f"""
DECLARE $EmbeddingList as List<Float>;

$EmbeddingString = Knn::ToBinaryStringFloat($EmbeddingList);

SELECT id, Knn::CosineDistance(embedding, $EmbeddingString) AS cosine
FROM {TABLE}
VIEW {INDEX}
ORDER BY cosine
LIMIT {K};
"""

def load_ydb_config():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    profile = config.get("active_profile", "default")
    profiles = config.get("profiles", {})
    prof = profiles.get(profile, {})
    endpoint = prof.get("endpoint")
    database = prof.get("database")
    return endpoint, database

def make_random_embedding():
    arr = np.random.randn(EMBEDDING_DIM).astype(float)
    return arr.tolist()

def run_query(pool):
    emb_list = make_random_embedding()
    def callee(session: ydb.QuerySession):
        return list(session.transaction(TX_MODE).execute(
            QUERY,
            commit_tx=True,
            parameters={
                "$EmbeddingList": ydb.TypedValue(emb_list, ydb.ListType(ydb.PrimitiveType.Float)),
            },
        ))
    result = pool.retry_operation_sync(callee)
    return result[0].rows

def pretty_print(rows):
    if not rows:
        print("No results.")
        return
    id_width = 8
    for row in rows:
        id_str = str(row.get('id', ''))[:id_width]
        cosine = row.get('cosine')
        cosine_str = "None" if cosine is None else f"{cosine:.5f}"
        print(f"{id_str:<{id_width}} | {cosine_str}")

def run_benchmark(pool):
    times = []
    query_counter = 0
    start_time = time.time()

    def submit_query(executor, futures_times):
        f = executor.submit(run_query, pool)
        futures_times[f] = time.time()
        return f

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures_times = {}
        in_flight = set()
        for _ in range(MAX_WORKERS):
            f = submit_query(executor, futures_times)
            in_flight.add(f)
        while in_flight:
            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            for future in done:
                submit_time = futures_times.pop(future)
                finish_time = time.time()
                query_time = finish_time - submit_time
                rows = future.result()
                query_counter += 1
                print(f"\n=== Query result #{query_counter} ({query_time:.3f}s) ===")
                pretty_print(rows)
                times.append(query_time)
                in_flight.remove(future)
                if finish_time - start_time < DURATION:
                    f_new = submit_query(executor, futures_times)
                    in_flight.add(f_new)
        total_duration = time.time() - start_time
    return query_counter, total_duration, times

def main():
    endpoint, database = load_ydb_config()
    print(f"Using endpoint: {endpoint}, database: {database}")
    driver = ydb.Driver(endpoint=endpoint, database=database)
    driver.wait(fail_fast=True, timeout=5)
    pool = ydb.QuerySessionPool(driver)
    try:
        query_counter, total_duration, times = run_benchmark(pool)
        print(f"\nCompleted {query_counter} queries in {total_duration:.3f} seconds")
        if times:
            print(f"Queries per second: {query_counter / total_duration:.2f}")
            print(f"Avg query time: {sum(times)/len(times):.3f} seconds")
    finally:
        driver.stop()

if __name__ == "__main__":
    main()
