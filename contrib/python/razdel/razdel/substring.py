
from .record import Record


class Substring(Record):
    __attributes__ = ['start', 'stop', 'text']

    def __init__(self, start, stop, text):
        self.start = start
        self.stop = stop
        self.text = text


def find_substrings(chunks, text):
    offset = 0
    for chunk in chunks:
        start = text.find(chunk, offset)
        stop = start + len(chunk)
        yield Substring(start, stop, chunk)
        offset = stop
