import ydb.tools.mnc.scheme.common as c

scheme = c.object_with_additional_fields(
    __name__='agent',
    hosts=c.list_with(str),
    port=c.optional(int),
    mnc_home=c.optional(str),
)
