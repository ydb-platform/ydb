from io import SEEK_CUR
from typing import IO

from pure_protobuf.interfaces._skip import Skip


class SkipFixed64(Skip):
    __slots__ = ()

    def __call__(self, io: IO[bytes]) -> None:
        io.seek(8, SEEK_CUR)


skip_fixed_64 = SkipFixed64()
