import io
import threading
from typing import Optional

from zstandard import ZstdCompressor  # type: ignore[import]

from langsmith import utils as ls_utils

compression_level = int(ls_utils.get_env_var("RUN_COMPRESSION_LEVEL") or 1)
compression_threads = int(ls_utils.get_env_var("RUN_COMPRESSION_THREADS") or -1)

DEFAULT_MAX_UNCOMPRESSED_QUEUE_BYTES = 1024 * 1024 * 1024  # 1GB


class CompressedTraces:
    def __init__(self, max_uncompressed_size_bytes: Optional[int] = None) -> None:
        # Configure the maximum total uncompressed size for the in-memory queue.
        if max_uncompressed_size_bytes is None:
            max_bytes_str = ls_utils.get_env_var("MAX_INGEST_MEMORY_BYTES")
            if max_bytes_str is not None:
                max_uncompressed_size_bytes = int(max_bytes_str)
            else:
                max_uncompressed_size_bytes = DEFAULT_MAX_UNCOMPRESSED_QUEUE_BYTES

        self.max_uncompressed_size_bytes = max_uncompressed_size_bytes

        self.buffer: io.BytesIO = io.BytesIO()
        self.trace_count: int = 0
        self.lock = threading.Lock()
        self.uncompressed_size: int = 0
        self._context: list[str] = []

        self.compressor_writer = ZstdCompressor(
            level=compression_level, threads=compression_threads
        ).stream_writer(self.buffer, closefd=False)

    def reset(self) -> None:
        self.buffer = io.BytesIO()
        self.trace_count = 0
        self.uncompressed_size = 0
        self._context = []
        self.compressor_writer = ZstdCompressor(
            level=compression_level, threads=-1
        ).stream_writer(self.buffer, closefd=False)
