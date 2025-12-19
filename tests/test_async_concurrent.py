import requests, threading, time, uuid
from datetime import datetime, timezone

BASE_URL = "http://localhost:8080"

def test_async_light_concurrent_publish():
    eid = f"evt-concurrent-{uuid.uuid4()}"

    payload = {
        "events": [{
            "topic": "async-concurrent",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        }]
    }

    def send():
        requests.post(f"{BASE_URL}/publish", json=payload)

    t1 = threading.Thread(target=send)
    t2 = threading.Thread(target=send)

    t1.start(); t2.start()
    t1.join(); t2.join()

    time.sleep(1)

    events = requests.get(
        f"{BASE_URL}/events?topic=async-concurrent"
    ).json()

    assert len(events) == 1


def test_async_multi_event_concurrency():
    def send():
        payload = {
            "events": [{
                "topic": "async-multi",
                "event_id": f"evt-{uuid.uuid4()}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "pytest",
                "payload": {}
            }]
        }
        requests.post(f"{BASE_URL}/publish", json=payload)

    threads = [threading.Thread(target=send) for _ in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()

    time.sleep(1)

    stats = requests.get(f"{BASE_URL}/stats").json()
    assert stats["unique_processed"] >= 5
