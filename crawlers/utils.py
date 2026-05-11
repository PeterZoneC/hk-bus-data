"""Shared HTTP client and utility helpers."""
import asyncio
import logging
import opencc

logger = logging.getLogger(__name__)

_tc2sc = opencc.OpenCC("t2s")

# Some government APIs (e.g. GMB) block requests without a browser-like User-Agent
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def tc_to_sc(text: str) -> str:
    """Convert Traditional Chinese to Simplified Chinese."""
    if not text:
        return text
    return _tc2sc.convert(text)


async def fetch_json(client, url: str, method: str = "GET", **kwargs) -> dict | list:
    """Fetch a URL and return parsed JSON, with simple retry logic."""
    for attempt in range(3):
        try:
            headers = {**HEADERS, **kwargs.pop("headers", {})}
            if method == "POST":
                r = await client.post(url, headers=headers, **kwargs)
            else:
                r = await client.get(url, headers=headers, **kwargs)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2:
                logger.error(f"Failed to fetch {url} after 3 attempts: {e}")
                raise
            wait = 2 ** attempt
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}, retrying in {wait}s")
            await asyncio.sleep(wait)
