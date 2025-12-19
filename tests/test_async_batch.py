import requests, time, uuid
from datetime import datetime, timezone

BASE_URL = "http://localhost:8080"

def test_async_batch_publish():
    events = []
    for _ in range(5):
        events.append({
            "topic": "async-batch",
            "event_id": f"evt-{uuid.uuid4()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        })

    r = requests.post(f"{BASE_URL}/publish", json={"events": events})
    assert r.json()["received"] == 5

    time.sleep(1)

    stats = requests.get(f"{BASE_URL}/stats").json()
    assert stats["unique_processed"] >= 5


def test_async_batch_with_internal_duplicate():
    eid = f"evt-batch-dup-{uuid.uuid4()}"

    events = [
        {
            "topic": "async-batch-dup",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        },
        {
            "topic": "async-batch-dup",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        }
    ]

    requests.post(f"{BASE_URL}/publish", json={"events": events})
    time.sleep(1)

    events = requests.get(
        f"{BASE_URL}/events?topic=async-batch-dup"
    ).json()

    assert len(events) == 1
