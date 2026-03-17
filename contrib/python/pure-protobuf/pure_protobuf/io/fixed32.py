from io import SEEK_CUR
from typing import IO

from pure_protobuf.interfaces._skip import Skip


class SkipFixed32(Skip):
    """Skip fixed 32-field record."""

    def __call__(self, io: IO[bytes]) -> None:
        io.seek(4, SEEK_CUR)


skip_fixed_32 = SkipFixed32()
