import requests, time

BASE_URL = "http://localhost:8080"

def test_async_stats_fields():
    stats = requests.get(f"{BASE_URL}/stats").json()

    assert "received" in stats
    assert "unique_processed" in stats
    assert "duplicate_dropped" in stats
    assert stats["mode"] == "async"


def test_async_stats_monotonic():
    s1 = requests.get(f"{BASE_URL}/stats").json()
    time.sleep(0.5)
    s2 = requests.get(f"{BASE_URL}/stats").json()

    assert s2["uptime_seconds"] >= s1["uptime_seconds"]
