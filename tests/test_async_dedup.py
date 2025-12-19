import requests, time, uuid
from datetime import datetime, timezone

BASE_URL = "http://localhost:8080"

def test_async_deduplication():
    eid = f"evt-dedup-{uuid.uuid4()}"

    payload = {
        "events": [{
            "topic": "async-dedup",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        }]
    }

    requests.post(f"{BASE_URL}/publish", json=payload)
    requests.post(f"{BASE_URL}/publish", json=payload)

    time.sleep(1.5)

    stats = requests.get(f"{BASE_URL}/stats").json()
    assert stats["unique_processed"] >= 1
    assert stats["duplicate_dropped"] >= 1


def test_async_idempotency_persist():
    eid = f"evt-idem-{uuid.uuid4()}"

    payload = {
        "events": [{
            "topic": "async-idem",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        }]
    }

    requests.post(f"{BASE_URL}/publish", json=payload)
    time.sleep(1)
    requests.post(f"{BASE_URL}/publish", json=payload)

    time.sleep(1)

    events = requests.get(
        f"{BASE_URL}/events?topic=async-idem"
    ).json()

    assert len(events) == 1
