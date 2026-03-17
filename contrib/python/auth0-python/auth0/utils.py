def is_async_available() -> bool:
    try:
        import asyncio

        import aiohttp

        return True
    except ImportError:  # pragma: no cover
        pass

    return False
