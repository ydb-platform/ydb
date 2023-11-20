import lz4.frame as lz4frame


def get_frame_info_check(compressed_data,
                         source_size,
                         store_size,
                         block_size,
                         block_linked,
                         content_checksum,
                         block_checksum):

    frame_info = lz4frame.get_frame_info(compressed_data)

    assert frame_info["content_checksum"] == content_checksum
    assert frame_info["block_checksum"] == block_checksum

    assert frame_info["skippable"] is False

    if store_size is True:
        assert frame_info["content_size"] == source_size
    else:
        assert frame_info["content_size"] == 0

    if source_size > frame_info['block_size']:
        # More than a single block
        assert frame_info["block_linked"] == block_linked

        if block_size == lz4frame.BLOCKSIZE_DEFAULT:
            assert frame_info["block_size_id"] == lz4frame.BLOCKSIZE_MAX64KB
        else:
            assert frame_info["block_size_id"] == block_size


def get_chunked(data, nchunks):
    size = len(data)
    # stride = int(math.ceil(float(size)/nchunks))  # no // on py 2.6
    stride = size // nchunks
    start = 0
    end = start + stride
    while end < size:
        yield data[start:end]
        start += stride
        end += stride
    yield data[start:]
