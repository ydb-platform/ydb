PROGRAM(ydb)

STRIP()

SRCS(
    main.cpp
)

DISABLE(USE_ASMLIB)

PEERDIR(
    contrib/libs/asmlib
    ydb/apps/ydb/commands
)

RESOURCE(
    ydb/apps/ydb/version.txt version.txt
)

#
# DON'T ALLOW NEW DEPENDENCIES WITHOUT EXPLICIT APPROVE FROM  kikimr-dev@ or fomichev@
#
CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY
    PEERDIRS
    build/platform
    certs
    contrib
    library
    tools/enum_parser/enum_parser
    tools/enum_parser/enum_serialization_runtime
    tools/rescompressor
    tools/rorescompiler
    util
    ydb/apps/ydb
    ydb/core/fq/libs/protos
    ydb/core/grpc_services/validation
    ydb/library
    ydb/public
    ydb/library/yql/public/decimal
    ydb/library/yql/public/issue
    ydb/library/yql/public/issue/protos
)

END()
