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

INCLUDE(${ARCADIA_ROOT}/devtools/large_on_multi_slots.inc)

END()
