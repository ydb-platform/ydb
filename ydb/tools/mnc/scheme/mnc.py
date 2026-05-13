import ydb.tools.mnc.scheme.common as c

scheme = c.object_with(
    __name__='mnc',
    arcadia_root=c.optional(str),
    git_ydb_root=c.optional(str),
    default_source=c.optional(('arcadia', 'git')),
    default_bin_kind=c.optional(('kikimr', 'ydbd')),
)
