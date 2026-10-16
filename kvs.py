import mini_kvs
import concurrent.futures

# 1. Initialize persistent store
db = mini_kvs.MiniKVS("store.aof")

# 2. Uses Python mapping protocols naturally
db["user:101"] = b'{"name": "Alice", "role": "admin"}'
db["user:102"] = b'{"name": "Bob", "role": "engineer"}'

print("Length:", len(db))
print("Contains user:101:", "user:101" in db)
print("Fetch user:101:", db["user:101"].decode())

# 3. High-concurrency read test (GIL released in Rust)
def reader(k):
    return db.get(k)

with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    keys = ["user:101", "user:102"] * 500
    results = list(pool.map(reader, keys))
    print(f"Executed {len(results)} concurrent reads without GIL contention.")

# 4. Crash-recovery demonstration
del db
db_recovered = mini_kvs.MiniKVS("store.aof")
print("Recovered user:102:", db_recovered["user:102"].decode())