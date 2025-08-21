PY2TEST()

TAG(ya:manual)

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
    ydb/library/yql/minikql/benchmark/pack_num
)

END()
