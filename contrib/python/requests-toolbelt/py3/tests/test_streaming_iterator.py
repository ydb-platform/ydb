import io

from requests_toolbelt.streaming_iterator import StreamingIterator

import pytest

@pytest.fixture(params=[True, False])
def get_iterable(request):
    '''
    When this fixture is used, the test is run twice -- once with the iterable
    being a file-like object, once being an iterator.
    '''
    is_file = request.param
    def inner(chunks):
        if is_file:
            return io.BytesIO(b''.join(chunks))
        return iter(chunks)
    return inner


class TestStreamingIterator(object):
    @pytest.fixture(autouse=True)
    def setup(self, get_iterable):
        self.chunks = [b'here', b'are', b'some', b'chunks']
        self.size = 17
        self.uploader = StreamingIterator(self.size, get_iterable(self.chunks))

    def test_read_returns_all_chunks_in_one(self):
        assert self.uploader.read() == b''.join(self.chunks)

    def test_read_returns_empty_string_after_exhausting_the_iterator(self):
        for i in range(0, 4):
            self.uploader.read(8192)

        assert self.uploader.read() == b''
        assert self.uploader.read(8192) == b''


class TestStreamingIteratorWithLargeChunks(object):
    @pytest.fixture(autouse=True)
    def setup(self, get_iterable):
        self.letters = [b'a', b'b', b'c', b'd', b'e']
        self.chunks = (letter * 2000 for letter in self.letters)
        self.size = 5 * 2000
        self.uploader = StreamingIterator(self.size, get_iterable(self.chunks))

    def test_returns_the_amount_requested(self):
        chunk_size = 1000
        bytes_read = 0
        while True:
            b = self.uploader.read(chunk_size)
            if not b:
                break
            assert len(b) == chunk_size
            bytes_read += len(b)

        assert bytes_read == self.size

    def test_returns_all_of_the_bytes(self):
        chunk_size = 8192
        bytes_read = 0
        while True:
            b = self.uploader.read(chunk_size)
            if not b:
                break
            bytes_read += len(b)

        assert bytes_read == self.size
