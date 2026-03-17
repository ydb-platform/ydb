"""
The ``zarr.core`` module is considered private API and should not be imported
directly by 3rd-party code.
"""

from __future__ import annotations

from zarr.core.buffer import Buffer, NDBuffer  # noqa: F401
from zarr.core.codec_pipeline import BatchedCodecPipeline  # noqa: F401
