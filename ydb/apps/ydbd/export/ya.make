LIBRARY()

SRCS(
    export.cpp
)

PEERDIR(
    yql/essentials/public/types
    ydb/core/tx/columnshard/engines/scheme/defaults/protos
    ydb/library/mkql_proto/protos
    ydb/library/aclib/protos/identity
    ydb/library/aclib/protos/acl
    ydb/library/formats/arrow/protos
    ydb/core/tx/datashard
)

END()
