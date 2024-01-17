import lz4.stream
import pytest
import sys


if sys.version_info < (3, ):
    from struct import pack, unpack

    def _get_format(length, byteorder, signed):
        _order = {'l': '<', 'b': '>'}
        _fmt = {1: 'b', 2: 'h', 4: 'i', 8: 'q'}
        _sign = {True: lambda x: x.lower(), False: lambda x: x.upper()}
        return _sign[signed](_order[byteorder[0].lower()] + _fmt[length])

    def int_to_bytes(value, length=4, byteorder='little', signed=False):
        return bytearray(pack(_get_format(length, byteorder, signed), value))

    def int_from_bytes(bytes, byteorder='little', signed=False):
        return unpack(_get_format(len(bytes), byteorder, signed), bytes)[0]

else:
    def int_to_bytes(value, length=4, byteorder='little', signed=False):
        return value.to_bytes(length, byteorder, signed=signed)

    def int_from_bytes(bytes, byteorder='little', signed=False):
        return int.from_bytes(bytes, byteorder, signed=signed)

# Out-of-band block size record tests


def test_round_trip():
    data = b"2099023098234882923049823094823094898239230982349081231290381209380981203981209381238901283098908123109238098123" * 24
    kwargs = {'strategy': "double_buffer", 'buffer_size': 256, 'store_comp_size': 4}

    oob_kwargs = {}
    oob_kwargs.update(kwargs)
    oob_kwargs['store_comp_size'] = 0

    ib_cstream = bytearray()
    oob_cstream = bytearray()
    oob_sizes = []

    with lz4.stream.LZ4StreamCompressor(**kwargs) as ib_proc, \
            lz4.stream.LZ4StreamCompressor(**oob_kwargs) as oob_proc:
        for start in range(0, len(data), kwargs['buffer_size']):
            chunk = data[start:start + kwargs['buffer_size']]
            ib_block = ib_proc.compress(chunk)
            oob_block = oob_proc.compress(chunk)

            assert (len(ib_block) == (len(oob_block) + kwargs['store_comp_size'])), \
                "Blocks size mismatch: "                                            \
                "{}/{}".format(len(ib_block), len(oob_block) + kwargs['store_comp_size'])

            assert (int_from_bytes(ib_block[:kwargs['store_comp_size']]) == len(oob_block)), \
                "Blocks size record mismatch: got {}, expected {}".format(
                    int_from_bytes(ib_block[:kwargs['store_comp_size']]),
                    len(oob_block))

            assert (ib_block[kwargs['store_comp_size']:] == oob_block), "Blocks data mismatch"

            ib_cstream += ib_block
            oob_cstream += oob_block
            oob_sizes.append(len(oob_block))

    ib_dstream = bytearray()
    oob_dstream = bytearray()

    with lz4.stream.LZ4StreamDecompressor(**kwargs) as ib_proc, \
            lz4.stream.LZ4StreamDecompressor(**oob_kwargs) as oob_proc:
        ib_offset = 0
        oob_index = 0
        oob_offset = 0
        while ib_offset < len(ib_cstream) and oob_index < len(oob_sizes):
            ib_block = ib_proc.get_block(ib_cstream[ib_offset:])
            oob_block = oob_cstream[oob_offset:oob_offset + oob_sizes[oob_index]]

            assert (len(ib_block) == len(oob_block)), \
                "Blocks size mismatch: {}/{}".format(len(ib_block), len(oob_block))

            assert (ib_block == oob_block), "Blocks data mismatch"

            ib_chunk = ib_proc.decompress(ib_block)
            oob_chunk = oob_proc.decompress(oob_block)

            assert (len(ib_chunk) == len(oob_chunk)), \
                "Chunks size mismatch: {}/{}".format(len(ib_chunk), len(oob_chunk))

            assert (ib_chunk == oob_chunk), "Chunks data mismatch"

            ib_dstream += ib_chunk
            oob_dstream += oob_chunk

            ib_offset += kwargs['store_comp_size'] + len(ib_block)
            oob_offset += oob_sizes[oob_index]
            oob_index += 1

    assert (len(ib_dstream) == len(oob_dstream)), "Decompressed streams length mismatch"

    assert (len(data) == len(ib_dstream)), "Decompressed streams length mismatch"

    assert (len(data) == len(oob_dstream)), "Decompressed streams length mismatch"

    assert (ib_dstream == oob_dstream), "Decompressed streams mismatch"

    assert (data == ib_dstream), "Decompressed streams mismatch"

    assert (data == oob_dstream), "Decompressed streams mismatch"


def test_invalid_usage():
    data = b"2099023098234882923049823094823094898239230982349081231290381209380981203981209381238901283098908123109238098123" * 24
    kwargs = {'strategy': "double_buffer", 'buffer_size': 256, 'store_comp_size': 0}

    cstream = bytearray()
    oob_sizes = []

    with lz4.stream.LZ4StreamCompressor(**kwargs) as proc:
        for start in range(0, len(data), kwargs['buffer_size']):
            chunk = data[start:start + kwargs['buffer_size']]
            block = proc.compress(chunk)
            cstream += block
            oob_sizes.append(len(block))

    message = r"^LZ4 context is configured for storing block size out-of-band$"

    with pytest.raises(lz4.stream.LZ4StreamError, match=message):
        dstream = bytearray()

        with lz4.stream.LZ4StreamDecompressor(**kwargs) as proc:
            offset = 0
            index = 0
            while offset < len(cstream):
                block = proc.get_block(cstream[offset:])
                chunk = proc.decompress(block)

                dstream += chunk

                offset += kwargs['store_comp_size'] + len(block)
                index += 1
