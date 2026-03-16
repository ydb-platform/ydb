__all__ = [
    "IS_FASTAPI_V_0_128_0_OR_NEWER",
]

from fastapi import __version__

IS_FASTAPI_V_0_128_0_OR_NEWER = tuple(int(part) for part in __version__.split(".") if part.isdigit()) >= (0, 128, 0)
