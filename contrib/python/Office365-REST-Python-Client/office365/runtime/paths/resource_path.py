from typing import Iterator


class ResourcePath(object):
    """OData resource path"""

    def __init__(self, key=None, parent=None):
        # type: (int|str, "ResourcePath") -> None
        self._key = key
        self._parent = parent

    def patch(self, key):
        if self._key is None:
            self._key = key
        return self

    def __iter__(self):
        # type: () -> Iterator["ResourcePath"]
        current = self
        while current:
            yield current
            current = current.parent

    def __repr__(self):
        return self.to_url()

    def __str__(self):
        return self.to_url()

    def __eq__(self, other):
        return self.to_url() == other.to_url()

    def to_url(self):
        # type: () -> str
        """Builds url"""
        segments = []
        for path in self:
            segments.insert(0, path.segment)
            if path.delimiter:
                segments.insert(0, path.delimiter)
        return "".join(segments)

    @property
    def parent(self):
        return self._parent

    @property
    def segment(self):
        return str(self._key)

    @property
    def delimiter(self):
        return "/"
