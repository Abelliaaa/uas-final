import requests
from datetime import datetime, timezone
import uuid

BASE_URL = "http://localhost:8080"

def test_async_publish_single_event():
    payload = {
        "events": [{
            "topic": "async",
            "event_id": f"evt-{uuid.uuid4()}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "pytest",
            "payload": {}
        }]
    }

    r = requests.post(f"{BASE_URL}/publish", json=payload)
    data = r.json()

    assert r.status_code == 200
    assert data["status"] == "queued"
    assert data["received"] == 1


def test_async_publish_empty_batch():
    r = requests.post(f"{BASE_URL}/publish", json={"events": []})
    data = r.json()

    assert r.status_code == 200
    assert data["received"] == 0
