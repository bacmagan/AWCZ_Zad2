import json
import os
import random
import time
from datetime import datetime, timedelta

output_dir = "data/stream"
os.makedirs(output_dir, exist_ok=True)

event_time = datetime.utcnow() - timedelta(minutes=2)
event_types = ["view", "cart", "purchase"]
categories = ["electronics", "books", "fashion", "home", "sports"]

def generate_event():
    return {
        "user_id": f"u{random.randint(1, 50)}",
        "event_type": random.choices(event_types, weights=[0.6, 0.25, 0.15])[0],
        "timestamp": (datetime.now() - timedelta(seconds=random.randint(0, 300)) - timedelta(hours=2)).isoformat(),
        "product_id": f"p{random.randint(100, 120)}",
        "category": random.choice(categories),
        "price": round(random.uniform(10, 1000), 2)
    }

while True:
    batch = [generate_event() for _ in range(50)]
    filename = os.path.join(output_dir, f"events_{int(time.time())}.json")
    with open(filename, "w") as f:
        for e in batch:
            f.write(json.dumps(e) + "\n")
    print(f"Wrote: {filename}")
    time.sleep(5)
