from pymongo import MongoClient
from datetime import datetime, timedelta
from config import Config
import random
import string

TOTAL_ROWS = 100_000
EXTRA_GROUPS = 10        # 10 groups
COLS_PER_GROUP = 25      # 25 cols per group => 250 extra cols

def rand_date(base: datetime) -> datetime:
    return base - timedelta(days=random.randint(0, 365))

def rand_str(n=8) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))

def init_filesystem_collection():
    client = MongoClient(Config.MONGODB_URI)
    db = client[Config.DATABASE_NAME]
    coll = db[Config.COLLECTION_NAME]

    coll.drop()

    base_date = datetime(2025, 1, 15)
    folders = ["Documents", "Images", "Downloads", "Music", "Videos", "Projects"]

    batch = []
    batch_size = 5_000
    inserted = 0

    for i in range(TOTAL_ROWS):
        doc = {
            "folder": random.choice(folders),
            "dateModified": rand_date(base_date),
            "size": random.randint(50_000, 50_000_000),
        }

        # 250 extra columns: g1_col1..g10_col25
        for g in range(1, EXTRA_GROUPS + 1):
            for c in range(1, COLS_PER_GROUP + 1):
                key = f"g{g}_col{c}"
                # mix of numeric and string values
                if c % 3 == 0:
                    doc[key] = random.randint(0, 10_000)
                elif c % 3 == 1:
                    doc[key] = rand_str(6)
                else:
                    doc[key] = round(random.random() * 1000, 3)

        batch.append(doc)

        if len(batch) >= batch_size:
            coll.insert_many(batch)
            inserted += len(batch)
            print(f"Inserted {inserted}/{TOTAL_ROWS} docs...")
            batch = []

    if batch:
        coll.insert_many(batch)
        inserted += len(batch)
        print(f"Inserted {inserted}/{TOTAL_ROWS} docs (final batch).")

    # Indexes for common operations
    coll.create_index("folder")
    coll.create_index("dateModified")
    coll.create_index("size")

    print("✓ Indexes created")
    print(f"✓ Done – {TOTAL_ROWS} documents with ~{EXTRA_GROUPS * COLS_PER_GROUP + 3} fields each")

if __name__ == "__main__":
    init_filesystem_collection()
