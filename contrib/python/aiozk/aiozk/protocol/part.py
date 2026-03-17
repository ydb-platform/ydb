import operator

from .primitives import Primitive


class Part:
    """
    Composable building block used to define Zookeeper protocol parts.

    Behaves much like the `Primitive` class but has named "sub parts"
    stored in a ``parts`` class attribute, that can hold any `Part` or
    `Primitive` subclass.
    """

    parts = ()

    def __init__(self, **kwargs):
        part_names = set([item[0] for item in self.parts])

        for name, value in kwargs.items():
            if name not in part_names:
                raise ValueError("Unknown part name: '%s'" % name)
            part_names.discard(name)

            setattr(self, name, value)

        for name in part_names:
            setattr(self, name, None)

    def render(self, parts=None):
        """
        Returns a two-element tuple with the ``struct`` format and values.

        Iterates over the applicable sub-parts and calls `render()` on them,
        accumulating the format string and values.

        Optionally takes a subset of parts to render, default behavior is to
        render all sub-parts belonging to the class.
        """
        if not parts:
            parts = self.parts

        fmt = []
        data = []

        for name, part_class in parts:
            if issubclass(part_class, Primitive):
                part = part_class(getattr(self, name, None))
            else:
                part = getattr(self, name, None)

            part_format, part_data = part.render()

            fmt.extend(part_format)
            data.extend(part_data)

        return ''.join(fmt), data

    @classmethod
    def parse(cls, buff, offset):
        """
        Given a buffer and offset, returns the parsed value and new offset.

        Calls `parse()` on the given buffer for each sub-part in order and
        creates a new instance with the results.
        """
        values = {}

        for name, part in cls.parts:
            value, new_offset = part.parse(buff, offset)

            values[name] = value
            offset = new_offset

        return cls(**values), offset

    def __eq__(self, other):
        """
        `Part` instances are equal if all of their sub-parts are also equal.
        """
        return all([getattr(self, part_name) == getattr(other, part_name) for part_name, part_class in self.parts])

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        def subpart_string(part_info):
            part_name, part_class = part_info

            if not part_class.__name__.startswith('VectorOf'):
                return '%s=%s' % (part_name, getattr(self, part_name, None))

            return '%s=[%s]' % (part_name, ', '.join([str(item) for item in getattr(self, part_name, [])]))

        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join([subpart_string(part) for part in sorted(self.parts, key=operator.itemgetter(0))]),
        )
