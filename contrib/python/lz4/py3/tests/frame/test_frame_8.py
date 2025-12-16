import threading
import lz4.frame as lz4frame


def test_lz4frame_open_write_read_text_iter(tmp_path):
    data = u'This is a test string'
    thread_id = threading.get_native_id()
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='wt') as fp:
        fp.write(data)
    data_out = ''
    with lz4frame.open(tmp_path / f'testfile_{thread_id}', mode='rt') as fp:
        for line in fp:
            data_out += line
    assert data_out == data
