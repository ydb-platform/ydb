import ydb.tools.mnc.scheme.common as c

scheme = c.object_with_additional_fields(
    __name__='mnc',
    git_ydb_root=c.optional(str),
)
