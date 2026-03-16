from .part import Part


response_xref = {}


class ResponseMeta(type):
    def __new__(cls, name, bases, attrs):
        new_class = super(ResponseMeta, cls).__new__(cls, name, bases, attrs)

        response_xref[new_class.opcode] = new_class

        return new_class


class Response(Part, metaclass=ResponseMeta):
    """
    Base class for all operation response classes.

    A simple class, has only an ``opcode`` attribute expected to be defined by
    subclasses, and a `deserialize()` classmethod.
    """

    opcode = None

    @classmethod
    def deserialize(cls, raw_bytes):
        """
        Deserializes the given raw bytes into an instance.

        Since this is a subclass of ``Part`` but a top-level one (i.e. no other
        subclass of ``Part`` would have a ``Response`` as a part) this merely
        has to parse the raw bytes and discard the resulting offset.
        """
        instance, _ = cls.parse(raw_bytes, offset=0)

        return instance
