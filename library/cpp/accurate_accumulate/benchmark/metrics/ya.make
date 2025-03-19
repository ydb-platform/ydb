PY2TEST()

SIZE(LARGE)

TAG(
    ya:force_sandbox
    sb:intel_e5_2660v1
    ya:fat
    ya:large_tests_on_multi_slots
    ya:large_tests_on_ya_make_2
)

TEST_SRCS(
    main.py
)

DEPENDS(
    library/cpp/accurate_accumulate/benchmark
)

END()
