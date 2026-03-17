Blosc Chunk Format
==================

The chunk is composed by a header and a blocks / splits section::

    +---------+--------+---------+
    |  header | blocks / splits  |
    +---------+--------+---------+

These are described below.

The header section
------------------

Blosc (as of Version 1.0.0) has the following 16 byte header that stores
information about the compressed buffer::

    |-0-|-1-|-2-|-3-|-4-|-5-|-6-|-7-|-8-|-9-|-A-|-B-|-C-|-D-|-E-|-F-|
      ^   ^   ^   ^ |     nbytes    |   blocksize   |    cbytes     |
      |   |   |   |
      |   |   |   +--typesize
      |   |   +------flags
      |   +----------versionlz
      +--------------version

Datatypes of the header entries
-------------------------------

All entries are little endian.

:version:
    (``uint8``) Blosc format version.
:versionlz:
    (``uint8``) Version of the internal compressor used.
:flags and compressor enumeration:
    (``bitfield``) The flags of the buffer

    :bit 0 (``0x01``):
        Whether the byte-shuffle filter has been applied or not.
    :bit 1 (``0x02``):
        Whether the internal buffer is a pure memcpy or not.
    :bit 2 (``0x04``):
        Whether the bit-shuffle filter has been applied or not.
    :bit 3 (``0x08``):
        Reserved, must be zero.
    :bit 4 (``0x10``):
        If set, the blocks will not be split in sub-blocks during compression.
    :bit 5 (``0x20``):
        Part of the enumeration for compressors.
    :bit 6 (``0x40``):
        Part of the enumeration for compressors.
    :bit 7 (``0x80``):
        Part of the enumeration for compressors.

    The last three bits form an enumeration that allows to use alternative
    compressors.

    :``0``:
        ``blosclz``
    :``1``:
        ``lz4`` or ``lz4hc``
    :``2``:
        ``snappy``
    :``3``:
        ``zlib``
    :``4``:
        ``zstd``

:typesize:
    (``uint8``) Number of bytes for the atomic type.
:nbytes:
    (``uint32``) Uncompressed size of the buffer (this header is not included).
:blocksize:
    (``uint32``) Size of internal blocks.
:cbytes:
    (``uint32``) Compressed size of the buffer (including this header).

The blocks / splits section
---------------------------

After the header, there come the blocks / splits section.  Blocks are equal-sized parts of the chunk, except for the last block that can be shorter or equal than the rest.

At the beginning of the blocks section, there come a list of `int32_t bstarts` to indicate where the different encoded blocks starts (counting from the end of this `bstarts` section)::

    +=========+=========+========+=========+
    | bstart0 | bstart1 |   ...  | bstartN |
    +=========+=========+========+=========+

Finally, it comes the actual list of compressed blocks / splits data streams.  It turns out that a block may optionally (see bit 4 in `flags` above) be further split in so-called splits which are the actual data streams that are transmitted to codecs for compression.  If a block is not split, then the split is equivalent to a whole block.  Before each split in the list, there is the compressed size of it, expressed as an `int32_t`::

    +========+========+========+========+========+========+========+
    | csize0 | split0 | csize1 | split1 |   ...  | csizeN | splitN |
    +========+========+========+========+========+========+========+


*Note*: all the integers are stored in little endian.

