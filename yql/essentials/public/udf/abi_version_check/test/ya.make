PY3TEST()

TEST_SRCS(
    test_abi_check.py
)

DEPENDS(
    yql/essentials/public/udf/abi_version_check/test/mixed_abi
    yql/essentials/public/udf/abi_version_check/test/single_abi
)

END()

RECURSE(
    current_probe
    mixed_abi
    single_abi
    stable_probe
)
