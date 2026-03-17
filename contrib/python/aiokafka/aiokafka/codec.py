import gzip
import io
import struct

from typing_extensions import Buffer

_XERIAL_V1_HEADER = (-126, b"S", b"N", b"A", b"P", b"P", b"Y", 0, 1, 1)
_XERIAL_V1_FORMAT = "bccccccBii"

try:
    import cramjam
except ImportError:
    cramjam = None


def has_gzip() -> bool:
    return True


def has_snappy() -> bool:
    return cramjam is not None


def has_zstd() -> bool:
    return cramjam is not None


def has_lz4() -> bool:
    return cramjam is not None


def gzip_encode(payload: Buffer, compresslevel: int | None = None) -> bytes:
    if not compresslevel:
        compresslevel = 9

    buf = io.BytesIO()

    # Gzip context manager introduced in python 2.7
    # so old-fashioned way until we decide to not support 2.6
    gzipper = gzip.GzipFile(fileobj=buf, mode="w", compresslevel=compresslevel)
    try:
        gzipper.write(payload)
    finally:
        gzipper.close()

    return buf.getvalue()


def gzip_decode(payload: Buffer) -> bytes:
    buf = io.BytesIO(payload)

    # Gzip context manager introduced in python 2.7
    # so old-fashioned way until we decide to not support 2.6
    gzipper = gzip.GzipFile(fileobj=buf, mode="r")
    try:
        return gzipper.read()
    finally:
        gzipper.close()


def snappy_encode(
    payload: Buffer, xerial_compatible: bool = True, xerial_blocksize: int = 32 * 1024
) -> bytes:
    """Encodes the given data with snappy compression.

    If xerial_compatible is set then the stream is encoded in a fashion
    compatible with the xerial snappy library.

    The block size (xerial_blocksize) controls how frequent the blocking occurs
    32k is the default in the xerial library.

    The format winds up being:


        +-------------+------------+--------------+------------+--------------+
        |   Header    | Block1 len | Block1 data  | Blockn len | Blockn data  |
        +-------------+------------+--------------+------------+--------------+
        |  16 bytes   |  BE int32  | snappy bytes |  BE int32  | snappy bytes |
        +-------------+------------+--------------+------------+--------------+


    It is important to note that the blocksize is the amount of uncompressed
    data presented to snappy at each block, whereas the blocklen is the number
    of bytes that will be present in the stream; so the length will always be
    <= blocksize.

    """

    if not has_snappy():
        raise NotImplementedError("Snappy codec is not available")

    if not xerial_compatible:
        return cramjam.snappy.compress_raw(payload)

    out = io.BytesIO()
    for fmt, dat in zip(_XERIAL_V1_FORMAT, _XERIAL_V1_HEADER, strict=False):
        out.write(struct.pack("!" + fmt, dat))

    payload = memoryview(payload)
    for chunk in (
        payload[i : i + xerial_blocksize]
        for i in range(0, len(payload), xerial_blocksize)
    ):
        block = cramjam.snappy.compress_raw(chunk)
        block_size = len(block)
        out.write(struct.pack("!i", block_size))
        out.write(block)

    return out.getvalue()


def _detect_xerial_stream(payload: Buffer) -> bool:
    """Detects if the data given might have been encoded with the blocking mode
    of the xerial snappy library.

    This mode writes a magic header of the format:
        +--------+--------------+------------+---------+--------+
        | Marker | Magic String | Null / Pad | Version | Compat |
        +--------+--------------+------------+---------+--------+
        |  byte  |   c-string   |    byte    |  int32  | int32  |
        +--------+--------------+------------+---------+--------+
        |  -126  |   'SNAPPY'   |     \0     |         |        |
        +--------+--------------+------------+---------+--------+

    The pad appears to be to ensure that SNAPPY is a valid cstring
    The version is the version of this format as written by xerial,
    in the wild this is currently 1 as such we only support v1.

    Compat is there to claim the minimum supported version that
    can read a xerial block stream, presently in the wild this is
    1.
    """

    payload = memoryview(payload)
    if len(payload) > 16:
        header = struct.unpack("!" + _XERIAL_V1_FORMAT, payload[:16])
        return header == _XERIAL_V1_HEADER
    return False


def snappy_decode(payload: Buffer) -> bytes:
    if not has_snappy():
        raise NotImplementedError("Snappy codec is not available")

    if _detect_xerial_stream(payload):
        # TODO ? Should become a fileobj ?
        out = io.BytesIO()
        byt = memoryview(payload)[16:]
        length = len(byt)
        cursor = 0

        while cursor < length:
            block_size = struct.unpack_from("!i", byt[cursor:])[0]
            # Skip the block size
            cursor += 4
            end = cursor + block_size
            out.write(cramjam.snappy.decompress_raw(byt[cursor:end]))
            cursor = end

        out.seek(0)
        return out.read()
    else:
        return bytes(cramjam.snappy.decompress_raw(payload))


def lz4_encode(payload: Buffer, level: int = 9) -> bytes:
    # level=9 is used by default by broker itself
    # https://cwiki.apache.org/confluence/display/KAFKA/KIP-390%3A+Support+Compression+Level
    if not has_lz4():
        raise NotImplementedError("LZ4 codec is not available")

    # Kafka broker doesn't support linked-block compression
    # https://cwiki.apache.org/confluence/display/KAFKA/KIP-57+-+Interoperable+LZ4+Framing
    compressor = cramjam.lz4.Compressor(
        level=level, content_checksum=False, block_linked=False
    )
    compressor.compress(payload)
    return bytes(compressor.finish())


def lz4_decode(payload: Buffer) -> bytes:
    if not has_lz4():
        raise NotImplementedError("LZ4 codec is not available")

    return bytes(cramjam.lz4.decompress(payload))


def zstd_encode(payload: Buffer, level: int | None = None) -> bytes:
    if not has_zstd():
        raise NotImplementedError("Zstd codec is not available")

    if level is None:
        # Default for kafka broker
        # https://cwiki.apache.org/confluence/display/KAFKA/KIP-390%3A+Support+Compression+Level
        level = 3

    return bytes(cramjam.zstd.compress(payload, level=level))


def zstd_decode(payload: Buffer) -> bytes:
    if not has_zstd():
        raise NotImplementedError("Zstd codec is not available")

    return bytes(cramjam.zstd.decompress(payload))
