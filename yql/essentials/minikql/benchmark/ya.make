# There is no reason to run benchmark for windows builds.
IF (NOT OS_WINDOWS)

RECURSE(
    pack_num
    pack_num/metrics
)

ENDIF()
