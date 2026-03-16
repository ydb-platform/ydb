from django.db.models.expressions import Value

from clickhouse_backend.models import fields

from .base import Func
from .tuples import Tuple

__all__ = [
    "halfMD5",
    "MD4",
    "MD5",
    "sipHash64",
    "sipHash64Keyed",
    "sipHash128",
    "sipHash128Keyed",
    "sipHash128Reference",
    "sipHash128ReferenceKeyed",
    "cityHash64",
    "intHash32",
    "intHash64",
    "SHA1",
    "SHA224",
    "SHA256",
    "SHA512",
    "SHA512_256",
    "BLAKE3",
    "URLHash",
    "farmFingerprint64",
    "farmHash64",
]


class halfMD5(Func):
    output_field = fields.UInt64Field()


class MD4(Func):
    arity = 1
    output_field = fields.FixedStringField(max_bytes=16)


class MD5(Func):
    arity = 1
    output_field = fields.FixedStringField(max_bytes=16)


class sipHash64(Func):
    output_field = fields.UInt64Field()


class sipHash64Keyed(Func):
    template = "%(function)s((%(k0)s, %(k1)s), %(expressions)s)"
    output_field = fields.UInt64Field()

    def __init__(self, k0, k1, *expressions):
        super().__init__(Tuple(k0, k1), *expressions)


class sipHash128(Func):
    output_field = fields.FixedStringField(max_bytes=16)


class sipHash128Keyed(sipHash64Keyed):
    output_field = fields.FixedStringField(max_bytes=16)


class sipHash128Reference(Func):
    output_field = fields.FixedStringField(max_bytes=16)


class sipHash128ReferenceKeyed(sipHash64Keyed):
    output_field = fields.FixedStringField(max_bytes=16)


class cityHash64(Func):
    output_field = fields.UInt64Field()


class intHash32(Func):
    arity = 1
    output_field = fields.UInt32Field()


class intHash64(Func):
    arity = 1
    output_field = fields.UInt64Field()


class SHA1(Func):
    arity = 1
    output_field = fields.FixedStringField(max_bytes=20)


class SHA224(Func):
    arity = 1
    output_field = fields.FixedStringField(max_bytes=28)


class SHA256(Func):
    arity = 1
    output_field = fields.FixedStringField(max_bytes=32)


class SHA512(Func):
    arity = 1
    output_field = fields.FixedStringField(max_bytes=64)


class SHA512_256(Func):
    arity = 1
    output_field = fields.FixedStringField(max_bytes=64)


class BLAKE3(Func):
    arity = 1
    output_field = fields.FixedStringField(max_bytes=32)


class URLHash(Func):
    output_field = fields.UInt64Field()

    def __init__(self, *expressions):
        if len(expressions) == 1:
            pass
        elif len(expressions) == 2:
            expressions = (expressions[0], Value(expressions[1]))
        else:
            raise TypeError(
                "'%s' takes 1 or 2 arguments (%s given)"
                % (
                    self.__class__.__name__,
                    len(expressions),
                )
            )
        super().__init__(*expressions)


class farmFingerprint64(Func):
    output_field = fields.UInt64Field()


class farmHash64(Func):
    output_field = fields.UInt64Field()
