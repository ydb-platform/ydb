SIZE(LARGE)
TIMEOUT(3600)

# use ya:not_autocheck and ya:manual with sanitizers until we have sufficient
# quota
IF (SANITIZER_TYPE)
    TAG(
        ya:fat
        ya:force_sandbox
        ya:not_autocheck
        ya:manual
        sb:ttl=3
        sb:logs_ttl=3
        sb:ssd
        sb:cores=64
    )
ELSE()
    TAG(
        ya:fat
        ya:force_sandbox
        sb:ttl=3
        sb:logs_ttl=3
        sb:ssd
        sb:cores=16
    )
ENDIF()

IF (ARCADIA_SANDBOX_SINGLESLOT)
    TAG(ya:large_tests_on_single_slots)
ENDIF()

FORK_SUBTESTS()
SPLIT_FACTOR(8)

REQUIREMENTS(
    cpu:8
    ram:30
)
