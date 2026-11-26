LIBRARY()

SRCDIR(yql/essentials/parser/pg_wrapper/interface)

SRCS(
    type_desc.h
)

PEERDIR(
    util
)

PEERDIR_FROM_POLICY(
    ALLOW
    statbox/qb4/py_libs/testing/update_sample/schema_helper_impl
    classifieds/vsdwh/tools/ci/jobs/yql_udf/parser
)

END()
