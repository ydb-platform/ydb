from ._stream import _create_context, _compress, _decompress, _get_block
from ._stream import LZ4StreamError, _compress_bound, _input_bound, LZ4_MAX_INPUT_SIZE  # noqa: F401


__doc__ = """\
A Python wrapper for the LZ4 stream protocol.

"""


class LZ4StreamDecompressor:
    """ LZ4 stream decompression context.

    """
    def __init__(self, strategy, buffer_size, return_bytearray=False, store_comp_size=4, dictionary=""):
        """ Instantiates and initializes a LZ4 stream decompression context.

            Args:
                strategy (str): Buffer management strategy. Can be: ``double_buffer``.
                buffer_size (int): Size of one buffer of the double-buffer used
                    internally for stream decompression in the case of ``double_buffer``
                    strategy.

            Keyword Args:
                return_bytearray (bool): If ``False`` (the default) then the function
                    will return a ``bytes`` object. If ``True``, then the function will
                    return a ``bytearray`` object.
                store_comp_size (int): Specify the size in bytes of the following
                    compressed block. Can be: ``0`` (meaning out-of-band block size),
                    ``1``, ``2`` or ``4`` (default: ``4``).
                dictionary (str, bytes or buffer-compatible object): If specified,
                    perform decompression using this initial dictionary.

            Raises:
                Exceptions occuring during the context initialization.

                OverflowError: raised if the ``dictionary`` parameter is too large
                    for the LZ4 context.
                ValueError: raised if some parameters are invalid.
                MemoryError: raised if some internal resources cannot be allocated.
                RuntimeError: raised if some internal resources cannot be initialized.

        """
        return_bytearray = 1 if return_bytearray else 0

        self._context = _create_context(strategy, "decompress", buffer_size,
                                        return_bytearray=return_bytearray,
                                        store_comp_size=store_comp_size,
                                        dictionary=dictionary)

    def __enter__(self):
        """ Enter the LZ4 stream context.

        """
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        """ Exit the LZ4 stream context.

        """
        pass

    def decompress(self, chunk):
        """ Decompress streamed compressed data.

            Decompress the given ``chunk``, using the given LZ4 stream context,
            Raises an exception if any error occurs.

            Args:
                chunk (str, bytes or buffer-compatible object): Data to decompress

            Returns:
                bytes or bytearray: Decompressed data.

            Raises:
                Exceptions occuring during decompression.

                ValueError: raised if the source is inconsistent with a finite LZ4
                    stream block chain.
                MemoryError: raised if the work output buffer cannot be allocated.
                OverflowError: raised if the source is too large for being decompressed
                    in the given context.
                LZ4StreamError: raised if the call to the LZ4 library fails. This can be
                    caused by ``decompressed_size`` being too small, or invalid data.

        """
        return _decompress(self._context, chunk)

    def get_block(self, stream):
        """ Return the first LZ4 compressed block from ``stream``.

            Args:
                stream (str, bytes or buffer-compatible object): LZ4 compressed stream.

            Returns:
                bytes or bytearray: LZ4 compressed data block.

            Raises:
                Exceptions occuring while getting the first block from ``stream``.

                BufferError: raised if the function cannot return a complete LZ4
                    compressed block from the stream (i.e. the stream does not hold
                    a complete block).
                MemoryError: raised if the output buffer cannot be allocated.
                OverflowError: raised if the source is too large for being handled by
                    the given context.
                LZ4StreamError: raised if used while in an out-of-band block size record
                    configuration.

        """
        return _get_block(self._context, stream)


class LZ4StreamCompressor:
    """ LZ4 stream compressing context.

    """
    def __init__(self, strategy, buffer_size, mode="default", acceleration=True, compression_level=9,
                 return_bytearray=False, store_comp_size=4, dictionary=""):
        """ Instantiates and initializes a LZ4 stream compression context.

            Args:
                strategy (str): Buffer management strategy. Can be: ``double_buffer``.
                buffer_size (int): Base size of the buffer(s) used internally for stream
                    compression/decompression. In the ``double_buffer`` strategy case,
                    this is the size of each buffer of the double-buffer.

            Keyword Args:
                mode (str): If ``default`` or unspecified use the default LZ4
                    compression mode. Set to ``fast`` to use the fast compression
                    LZ4 mode at the expense of compression. Set to
                    ``high_compression`` to use the LZ4 high-compression mode at
                    the expense of speed.
                acceleration (int): When mode is set to ``fast`` this argument
                    specifies the acceleration. The larger the acceleration, the
                    faster the but the lower the compression. The default
                    compression corresponds to a value of ``1``.
                compression_level (int): When mode is set to ``high_compression`` this
                    argument specifies the compression. Valid values are between
                    ``1`` and ``12``. Values between ``4-9`` are recommended, and
                    ``9`` is the default. Only relevant if ``mode`` is
                    ``high_compression``.
                return_bytearray (bool): If ``False`` (the default) then the function
                    will return a bytes object. If ``True``, then the function will
                    return a bytearray object.
                store_comp_size (int): Specify the size in bytes of the following
                    compressed block. Can be: ``0`` (meaning out-of-band block size),
                    ``1``, ``2`` or ``4`` (default: ``4``).
                dictionary (str, bytes or buffer-compatible object): If specified,
                    perform compression using this initial dictionary.

            Raises:
                Exceptions occuring during the context initialization.

                OverflowError: raised if the ``dictionary`` parameter is too large
                    for the LZ4 context.
                ValueError: raised if some parameters are invalid.
                MemoryError: raised if some internal resources cannot be allocated.
                RuntimeError: raised if some internal resources cannot be initialized.

        """
        return_bytearray = 1 if return_bytearray else 0

        self._context = _create_context(strategy, "compress", buffer_size,
                                        mode=mode,
                                        acceleration=acceleration,
                                        compression_level=compression_level,
                                        return_bytearray=return_bytearray,
                                        store_comp_size=store_comp_size,
                                        dictionary=dictionary)

    def __enter__(self):
        """ Enter the LZ4 stream context.

        """
        return self

    def __exit__(self, exc_type, exc, exc_tb):
        """ Exit the LZ4 stream context.

        """
        pass

    def compress(self, chunk):
        """ Stream compress given ``chunk`` of data.

            Compress the given ``chunk``, using the given LZ4 stream context,
            returning the compressed data as a ``bytearray`` or as a ``bytes`` object.

            Args:
                chunk (str, bytes or buffer-compatible object): Data to compress

            Returns:
                bytes or bytearray: Compressed data.

            Raises:
                Exceptions occuring during compression.

                OverflowError: raised if the source is too large for being compressed in
                    the given context.
                LZ4StreamError: raised if the call to the LZ4 library fails.

        """
        return _compress(self._context, chunk)
