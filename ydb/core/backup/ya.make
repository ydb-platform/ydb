RECURSE(
    common
    common/proto
    controller
    impl
    regexp
    tools
)

RECURSE_FOR_TESTS(
    common/ut
    regexp/ut
)
