# There is no reason to run benchmark for windows builds.
# There is no reason to run benchmarks for sanitizer builds since
# these targets take too much time to run.
IF (NOT OS_WINDOWS AND NOT SANITIZER_TYPE)

RECURSE(
    pack_num
    pack_num/metrics
)

ENDIF()
