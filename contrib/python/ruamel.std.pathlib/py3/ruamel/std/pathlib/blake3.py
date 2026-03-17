# this fails immediately on import of ruamel.std.pathlib.blake3 if blake3 not installed
# or if not the new version (older version had different thread setting argument)
# if size is set, this is assumed to be relatively small, it is excecuted
# in a single thread to prevent the threading overhead as per blake3 suggestiono
# on 10K size "header" this is slightly less than twice the speed compared to multi-threaded

__all__ = ['blake3']

from typing import Any
from pathlib import Path

import blake3

assert [int(x) for x in blake3.__version__.split('.')] > [
    0,
    3,
], f'need blake3 version > 0.3 (found: {blake3.__version__})'
b3 = blake3.blake3


if not hasattr(Path, 'blake3'):

    def _blake3(self: Path, size: int = -1, _bufsize: int = 2 ** 18) -> Any:
        """blake3 hash of the contents
        if size is provided and non-negative only read that amount of bytes from
        the start of the file
        """
        with self.open(mode='rb') as f:
            # from file_digest
            # binary file, socket.SocketIO object
            # Note: socket I/O uses different syscalls than file I/O.
            if size != -1:
                digestobj = b3()
                digestobj.update(f.read(size))
                return digestobj
            digestobj = b3(max_threads=b3.AUTO)
            buf = bytearray(_bufsize)  # Reusable buffer to reduce allocations.
            view = memoryview(buf)  # tested that this indeed speeds things up with blake3
            while True:
                readsize = f.readinto(buf)
                if readsize == 0:
                    break  # EOF
                digestobj.update(view[:readsize])
        return digestobj

    Path.blake3 = _blake3  # type: ignore
