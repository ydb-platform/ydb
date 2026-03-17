
from razdel.record import Record
from razdel.rule import JOIN
from razdel.substring import find_substrings


def safe_next(iter):
    try:
        return next(iter)
    except StopIteration:
        return


class Segmenter(Record):
    __attributes__ = ['split', 'rules']

    def __init__(self, split, rules):
        self.split = split
        self.rules = rules

    def join(self, split):
        for rule in self.rules:
            action = rule(split)
            if action:
                return action == JOIN

    def segment(self, parts):
        buffer = safe_next(parts)
        if buffer is None:
            return

        for split in parts:
            right = next(parts)
            split.buffer = buffer
            if self.join(split):
                buffer = buffer + split.delimiter + right
            else:
                yield buffer + split.delimiter
                buffer = right
        yield buffer

    post = None

    def __call__(self, text):
        parts = self.split(text)
        chunks = self.segment(parts)
        if self.post:
            chunks = self.post(chunks)
        return find_substrings(chunks, text)


class DebugSegmenter(Segmenter):
    def join(self, split):
        print(split.left, '|', split.delimiter, '|', split.right)
        for rule in self.rules:
            action = rule(split)
            if action:
                print('\t', action, rule.name)
                return action == JOIN
