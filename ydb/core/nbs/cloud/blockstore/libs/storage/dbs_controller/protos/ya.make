PROTO_LIBRARY()

EXCLUDE_TAGS(
    GO_PROTO
    JAVA_PROTO
)

SRCS(
    dbs_controller.proto
)

#CPP_PROTO_PLUGIN0(validation ydb/public/lib/validation)

END()
