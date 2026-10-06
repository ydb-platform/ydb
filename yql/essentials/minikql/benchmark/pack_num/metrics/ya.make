# Requires pack_num.
NO_BUILD_IF(OS_WINDOWS)

PY3TEST()

SIZE(LARGE)

TAG(
    ya:force_sandbox
    sb:intel_e5_2660v1
    ya:fat
)

TEST_SRCS(
    main.py
)

DEPENDS(
    yql/essentials/minikql/benchmark/pack_num
)


END()
