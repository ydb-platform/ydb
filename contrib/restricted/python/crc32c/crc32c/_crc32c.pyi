from typing_extensions import Buffer

big_endian: int
hardware_based: bool

def crc32(data: Buffer, value: int = 0, gil_release_mode: int = -1) -> int: ...
def crc32c(data: Buffer, value: int = 0, gil_release_mode: int = -1) -> int: ...
