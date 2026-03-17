
from yargy.record import Record


class Span(Record):
    __attributes__ = ['start', 'stop']

    def __init__(self, start, stop):
        self.start = start
        self.stop = stop

    def __eq__(self, other):
        if type(other) == tuple:
            return tuple(self) == other
        return Record.__eq__(self, other)

    def __lt__(self, other):
        return tuple(self) < tuple(other)

    def __repr__(self):
        return '[{self.start}, {self.stop})'.format(self=self)

    def _repr_pretty_(self, printer, cycle):
        printer.text(repr(self))


def get_nexts(spans):
    for _, stop in spans:
        for index, (start, _) in enumerate(spans):
            if start >= stop:
                yield index
                break
        else:
            yield


def span_size(span):
    start, stop = span
    return stop - start


def resolve_spans(spans):
    # Max coverage spans, used in parser.findall
    # https://stackoverflow.com/questions/19850580/maximum-non-overlapping-intervals-in-a-interval-tree
    #
    #    |-----|
    #      |----------|
    #          |------|
    # =>
    #    |-----|
    #          |------|
    #

    if not spans:
        return

    nexts = list(get_nexts(spans))
    covers = [0] * len(spans)
    pointers = [None] * len(spans)
    size = len(spans)
    for index in reversed(range(size)):
        span = spans[index]
        if index == size - 1:
            covers[index] = span_size(span)
            pointers[index] = {index}
        else:
            previous = covers[index + 1]
            cover = span_size(span)
            indexes = {index}
            next = nexts[index]
            if next:
                cover += covers[next]
                indexes |= pointers[next]
            if cover < previous:
                cover = previous
                indexes = pointers[index + 1]
            covers[index] = cover
            pointers[index] = indexes

    indexes = pointers[0]
    for index in sorted(indexes):
        yield spans[index]
