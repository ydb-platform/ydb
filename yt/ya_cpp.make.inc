# Common defaults for YT C++ projects
INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (SANITIZER_TYPE)
    CFLAGS(-gline-tables-only)
ENDIF()

IF (NOT OPENSOURCE)
    SET(YT_ENABLE_SEQUOIA 1)
ENDIF()
