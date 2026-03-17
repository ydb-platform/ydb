"""Provide compatibility over different versions of asyncio."""

import asyncio

if hasattr(asyncio, "async"):
    # Compatibility for Python 3.3 and older
    ensure_future = getattr(asyncio, "async")
else:
    ensure_future = asyncio.ensure_future