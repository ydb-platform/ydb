import lz4.frame as lz4frame


def test_lz4frame_open_write_read_text_iter():
    data = u'This is a test string'
    with lz4frame.open('testfile', mode='wt') as fp:
        fp.write(data)
    data_out = ''
    with lz4frame.open('testfile', mode='rt') as fp:
        for line in fp:
            data_out += line
    assert data_out == data
