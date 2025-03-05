PY3_LIBRARY()

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
ENDIF()

PY_SRCS(
    base.py
    select_missing_database.py
    select_missing_table.py
    select_positive_common.py
)

PEERDIR(
    yql/essentials/providers/common/proto
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/public/api/protos
)

END()
