UNITTEST()

SRCS(
    test_port_manager.cpp
)

SIZE(LARGE)

# We need to run tests at the same time on the single machine
FORK_SUBTESTS()

TAG(
    ya:fat
    ya:force_sandbox
    ya:large_tests_on_multi_slots
    ya:large_tests_on_ya_make_2
)

END()
