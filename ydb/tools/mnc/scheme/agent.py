import ydb.tools.mnc.scheme.common as c

scheme = c.object_with_additional_fields(
    __name__='agent',
    hosts=c.list_with(str),
    port=c.optional(int),
    mnc_home=c.optional(str),
    managed_partlabels=c.optional(c.list_with(str)),
    managed_partlabel_regexps=c.optional(c.list_with(str)),
    disks=c.optional(c.object_with_additional_fields(
        managed_partlabels=c.optional(c.list_with(str)),
        managed_partlabel_regexps=c.optional(c.list_with(str)),
        allowed_partlabel_regexps=c.optional(c.list_with(str)),
    )),
)
