LIBRARY()

SRCS(
    flags.cpp
)

PEERDIR(
    util
)

PEERDIR_FROM_POLICY(
    ALLOW
    cml/util/yql_split_code/lib
    classifieds/vsdwh/tools/ci/jobs/yql_udf/parser
)

END()
