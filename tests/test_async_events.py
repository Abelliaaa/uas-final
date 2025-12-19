import requests, time, uuid
from datetime import datetime, timezone

BASE_URL = "http://localhost:8080"

def test_async_event_visible():
    payload = {
        "events": [{
            "topic": "async-visible",
            "event_id": f"evt-{uuid.uuid4()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        }]
    }

    requests.post(f"{BASE_URL}/publish", json=payload)
    time.sleep(1)

    events = requests.get(
        f"{BASE_URL}/events?topic=async-visible"
    ).json()

    assert len(events) >= 1


def test_async_topic_isolation():
    eid = f"evt-topic-{uuid.uuid4()}"

    events = [
        {
            "topic": "async-a",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        },
        {
            "topic": "async-b",
            "event_id": eid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        }
    ]

    requests.post(f"{BASE_URL}/publish", json={"events": events})
    time.sleep(1)

    a = requests.get(f"{BASE_URL}/events?topic=async-a").json()
    b = requests.get(f"{BASE_URL}/events?topic=async-b").json()

    assert len(a) == 1
    assert len(b) == 1
