import warnings

from faststream.__about__ import (
    __version__ as FASTSTREAM_VERSION,  # noqa: N812
)

from dishka import FromDishka

warnings.warn(
    "The integration has been moved to the dishka-faststream package"
    " and will be removed in future versions."
    "\n`pip install dishka-faststream`"
    "\nhttps://github.com/faststream-community/dishka-faststream",
    DeprecationWarning,
    stacklevel=2,
)

FASTSTREAM_05 = FASTSTREAM_VERSION.startswith("0.5")
FASTSTREAM_06 = FASTSTREAM_VERSION.startswith("0.6")

if FASTSTREAM_05:
    from .faststream_05 import FastStreamProvider, inject, setup_dishka
elif FASTSTREAM_06:
    from .faststream_06 import (  # type: ignore[assignment]
        FastStreamProvider,
        inject,
        setup_dishka,
    )
else:
    raise RuntimeError(  # noqa: TRY003
        f"FastStream {FASTSTREAM_VERSION} version not supported",
    )

__all__ = (
    "FastStreamProvider",
    "FromDishka",
    "inject",
    "setup_dishka",
)
