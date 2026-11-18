UNITTEST_FOR(ydb/core/tx/columnshard/stats/scan/reader)

SRCS(report_ut.cpp program_observer_ut.cpp decode_observer_ut.cpp)


PEERDIR(
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

SRCS(io_observer_ut.cpp)

END()
