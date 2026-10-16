UNITTEST_FOR(ydb/core/config/validation)

REQUIREMENTS(cpu:1)
SRCS(
    ut/composite_conveyor_validator_ut.cpp
    validators_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
