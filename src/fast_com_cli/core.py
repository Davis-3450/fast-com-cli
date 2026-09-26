import asyncio
import re
import time
from typing import Any

import httpx

FAST_COM_URL = "https://fast.com"
API_URL = "https://api.fast.com/netflix/speedtest/v2"
DEFAULT_TOKEN = "YXNkZmFzZGxmbnNkYWZoYXNkZmhrYWxm"
CHUNK_SIZE = 65536
RAMPUP_S = 1.0
UPLOAD_CHUNK = 256 * 1024


class SpeedTestError(Exception):
    pass


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    data = sorted(values)
    idx = min(int(q * (len(data) - 1) + 0.5), len(data) - 1)
    return data[idx]


async def get_token(client: httpx.AsyncClient) -> str:
    try:
        r = await client.get(FAST_COM_URL, follow_redirects=True)
        r.raise_for_status()
        m = re.search(r'src="/(app-[^"]+\.js)"', r.text)
        if not m:
            return DEFAULT_TOKEN
        r2 = await client.get(f"{FAST_COM_URL}/{m.group(1)}")
        r2.raise_for_status()
        m = re.search(r'token:"([^"]+)"', r2.text)
        return m.group(1) if m else DEFAULT_TOKEN
    except httpx.HTTPError:
        return DEFAULT_TOKEN


async def get_targets(
    client: httpx.AsyncClient, token: str, url_count: int
) -> dict[str, Any]:
    r = await client.get(
        API_URL,
        params={"https": "true", "token": token, "urlCount": str(url_count)},
    )
    r.raise_for_status()
    data = r.json()
    targets = data.get("targets") or []
    if not targets:
        raise SpeedTestError("fast.com API returned no servers")
    servers = [
        {
            "url": t["url"].replace("speedtest", "speedtest/range/"),
            "location": t.get("location", {}),
        }
        for t in targets
    ]
    for s in servers:
        s["ping_url"] = s["url"].replace("/range/", "/range/0-0")
    return {"client": data.get("client", {}), "servers": servers}


async def ping(client: httpx.AsyncClient, url: str) -> float:
    start = time.perf_counter()
    await client.post(url)
    return (time.perf_counter() - start) * 1000


def _rolling_mbps(samples: list[tuple[float, float]], now: float, window: float = 1.5) -> float:
    if len(samples) < 2:
        return 0.0
    cutoff = now - window
    # Filter samples in the rolling window
    window_samples = [s for s in samples if s[0] >= cutoff]
    if len(window_samples) < 2:
        # If not enough samples in window, take the last few
        window_samples = samples[-10:] if len(samples) >= 10 else samples

    if len(window_samples) < 2:
        return 0.0

    span = window_samples[-1][0] - window_samples[0][0]
    # Require at least 0.25s of data to avoid division by tiny fractions of a second
    if span < 0.25:
        return 0.0

    nbytes = sum(b for _, b in window_samples)
    return nbytes * 8 / span / 1e6


async def measure_download(
    client: httpx.AsyncClient,
    servers: list[dict],
    duration: float,
    on_progress=None,
) -> tuple[float, list[float]]:
    deadline = time.perf_counter() + duration
    samples: list[tuple[float, float]] = []
    loaded_latencies: list[float] = []
    ping_urls = [s["ping_url"] for s in servers]

    async def pinger():
        i = 0
        while time.perf_counter() < deadline:
            t0 = time.perf_counter()
            try:
                await client.post(ping_urls[i % len(ping_urls)])
                loaded_latencies.append((time.perf_counter() - t0) * 1000)
            except httpx.HTTPError:
                pass
            i += 1
            await asyncio.sleep(1.0)

    async def worker(url: str):
        range_url = url.replace("/range/", "/range/0-2000000")
        while time.perf_counter() < deadline:
            try:
                async with client.stream("GET", range_url) as resp:
                    async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                        now = time.perf_counter()
                        samples.append((now, len(chunk)))
                        if on_progress:
                            on_progress(samples, now)
                        if now >= deadline:
                            return
            except httpx.HTTPError:
                return

    ping_task = asyncio.create_task(pinger())
    try:
        await asyncio.gather(
            *(worker(s["url"]) for s in servers), return_exceptions=True
        )
    finally:
        ping_task.cancel()

    if len(samples) < 2:
        raise SpeedTestError("No download data received")
    start_t = samples[0][0]
    end_t = samples[-1][0]
    window = end_t - start_t
    if window <= RAMPUP_S:
        bytes_used = sum(b for _, b in samples)
        effective = window
    else:
        bytes_used = 0
        for t, b in samples:
            if t > start_t + RAMPUP_S:
                bytes_used += b
        effective = window - RAMPUP_S
    mbps = bytes_used * 8 / effective / 1e6 if effective > 0 else 0.0
    return mbps, loaded_latencies


def _upload_payload(size: int) -> bytes:
    block = bytes(UPLOAD_CHUNK)
    reps, rem = divmod(size, UPLOAD_CHUNK)
    return block * reps + block[:rem]


async def measure_upload(
    client: httpx.AsyncClient,
    servers: list[dict],
    duration: float,
    download_mbps: float,
    on_progress=None,
) -> float:
    payload_size = max(int(download_mbps * 1e6 * duration / 8 * 1.25), 1_000_000)
    payload = _upload_payload(min(payload_size, 100_000_000))
    deadline = time.perf_counter() + duration * 2
    samples: list[tuple[float, float]] = []
    counter = [0]

    async def content():
        for off in range(0, len(payload), UPLOAD_CHUNK):
            if time.perf_counter() > deadline:
                return
            block = payload[off : off + UPLOAD_CHUNK]
            counter[0] += len(block)
            now = time.perf_counter()
            samples.append((now, len(block)))
            if on_progress:
                on_progress(samples, now)
            yield block

    async def worker(server: dict):
        try:
            await client.post(server["ping_url"], content=content())
        except httpx.HTTPError:
            pass

    start_t = time.perf_counter()
    await asyncio.gather(*(worker(s) for s in servers))
    end_t = time.perf_counter()
    if len(samples) < 2:
        raise SpeedTestError("Could not send upload data")
    window = end_t - start_t
    if window <= RAMPUP_S:
        bytes_used = counter[0]
        effective = window
    else:
        bytes_used = sum(b for t, b in samples if t > start_t + RAMPUP_S)
        effective = window - RAMPUP_S
    return bytes_used * 8 / effective / 1e6 if effective > 0 else 0.0


async def run_test(
    url_count: int = 3,
    download_duration: float = 10.0,
    upload_duration: float = 8.0,
    skip_upload: bool = False,
    on_status=None,
    on_progress=None,
) -> dict[str, Any]:
    def status(stage_id: str, message: str, meta: dict | None = None) -> None:
        if on_status:
            on_status(stage_id, message, meta or {})

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, read=max(download_duration, 15.0) + 10.0),
        limits=httpx.Limits(max_connections=20),
        headers={"User-Agent": "fast-com-cli/0.1"},
    ) as client:
        status("token", "Authenticating session with fast.com...")
        token = await get_token(client)

        status("targets", "Discovering nearby CDN edge nodes...")
        result = await get_targets(client, token, url_count)
        servers = result["servers"]
        client_info = result.get("client", {})

        status("ping", "Measuring baseline network latency...", {"client": client_info, "servers": servers})
        unloaded = []
        for s in servers[:2]:
            for _ in range(3):
                try:
                    unloaded.append(await ping(client, s["ping_url"]))
                except httpx.HTTPError:
                    pass

        status("download", "Streaming high-concurrency download payload...", {"client": client_info, "servers": servers})
        download_mbps, loaded = await measure_download(
            client,
            servers,
            download_duration,
            on_progress=lambda s, now: on_progress("download", s, now) if on_progress else None,
        )

        upload_mbps = None
        if not skip_upload:
            status("upload", "Executing upload throughput benchmark...", {"client": client_info, "servers": servers})
            upload_mbps = await measure_upload(
                client,
                servers,
                upload_duration,
                download_mbps,
                on_progress=lambda s, now: on_progress("upload", s, now) if on_progress else None,
            )
        else:
            status("upload_skip", "Upload benchmark skipped by configuration", {"client": client_info, "servers": servers})

    latency = min(unloaded) if unloaded else 0.0
    loaded_latency = _percentile(loaded, 0.75) if loaded else 0.0

    server_locations = sorted(
        {
            f"{s['location'].get('city', '?')}, {s['location'].get('country', '?')}"
            for s in servers
        }
    )
    return {
        "download_mbps": round(download_mbps, 1),
        "upload_mbps": round(upload_mbps, 1) if upload_mbps is not None else None,
        "latency_ms": round(latency),
        "loaded_latency_ms": round(loaded_latency) if loaded_latency else None,
        "client_ip": client_info.get("ip"),
        "client_location": client_info.get("location"),
        "servers": server_locations,
    }
