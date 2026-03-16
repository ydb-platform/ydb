EXECTEST()

VERSION(1.0.0)

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

RUN(fastops_test)

DEPENDS(contrib/libs/fastops/fastops/ut/bin)

SIZE(LARGE)

TAG(
    ya:fat
    ya:force_sandbox
    sb:intel_e5_2660v4
    ya:large_tests_on_single_slots
)

END()

RECURSE(
    bin
)
