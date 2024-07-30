PROGRAM(ydb)

STRIP()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/apps/ydb/commands
)

RESOURCE(
    ydb/apps/ydb/version.txt version.txt
)

IF (NOT USE_SSE4 AND NOT OPENSOURCE)
    # contrib/libs/glibasm can not be built without SSE4
    # Replace it with contrib/libs/asmlib which can be built this way.
    DISABLE(USE_ASMLIB)
    PEERDIR(
        contrib/libs/asmlib
    )
ENDIF()

#
# DON'T ALLOW NEW DEPENDENCIES WITHOUT EXPLICIT APPROVE FROM  kikimr-dev@ or fomichev@
#
CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY
    PEERDIRS
    build/internal/platform
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

IF (OS_LINUX)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()

