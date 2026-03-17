# Explicitly "import ... as" to make mypy --strict happy
from ._cli import main
from ._crc32c import big_endian as big_endian
from ._crc32c import crc32 as crc32
from ._crc32c import crc32c as crc32c
from ._crc32c import hardware_based as hardware_based
from ._crc32hash import CRC32CHash as CRC32CHash
