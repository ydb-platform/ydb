from __future__ import annotations

from typing import TYPE_CHECKING

from ._crc32c import crc32c

if TYPE_CHECKING:
    from typing_extensions import Buffer, Self


class CRC32CHash:
    """Wrapper class for crc32c. Tries to conform to the interface of `hashlib` classes."""

    @property
    def digest_size(self) -> int:
        """
        The size of the resulting hash in bytes.
        """
        return 4

    @property
    def block_size(self) -> int:
        """
        The internal block size of the hash algorithm in bytes.
        """
        return 1

    @property
    def name(self) -> str:
        """
        The canonical name of this hash,
        """
        return "crc32c"

    @property
    def checksum(self) -> int:
        """
        The checksum calculated so far. Not part of the hashlib interface.
        """
        return self._checksum

    def __init__(self, data: Buffer = b"", gil_release_mode: int = -1) -> None:
        """
        Initialise the hash object with an optional bytes-like object.
        Uses the given GIL release mode on each checksum calculation.
        """
        self._checksum = crc32c(data, gil_release_mode=gil_release_mode)
        self._gil_release_mode = gil_release_mode

    def update(self, data: Buffer) -> None:
        """
        Update the hash object with the bytes-like object.
        Repeated calls are equivalent to a single call with the concatenation of all the arguments:
        m.update(a); m.update(b) is equivalent to m.update(a+b).
        """
        self._checksum = crc32c(
            data, self._checksum, gil_release_mode=self._gil_release_mode
        )

    def digest(self) -> bytes:
        """
        Return the digest of the data passed to the update() method so far.
        This is a bytes object of size digest_size which may contain bytes in the whole range from 0 to 255.
        """
        return self._checksum.to_bytes(4, "big")

    def hexdigest(self) -> str:
        """
        Like digest() except the digest is returned as a string object of double length,
        containing only hexadecimal digits.
        This may be used to exchange the value safely in email or other non-binary environments.
        """
        return self.digest().hex()

    def copy(self) -> Self:
        """
        Return a copy (“clone”) of the hash object. This can be used to efficiently compute
        the digests of data sharing a common initial substring.
        """
        res = type(self)()
        res._checksum = self._checksum
        res._gil_release_mode = self._gil_release_mode
        return res
