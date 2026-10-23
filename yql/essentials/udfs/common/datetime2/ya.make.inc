YQL_ABI_VERSION(
    2
    47
    0
)

SRCS(
    yql/essentials/udfs/common/datetime2/datetime_udf.cpp
)

PEERDIR(
    util/draft
    library/cpp/type_info/tz
    yql/essentials/public/udf/arrow
    yql/essentials/public/langver
    yql/essentials/core/langver
    yql/essentials/minikql/datetime
)
