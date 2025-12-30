UNITTEST_FOR(util)

NO_BUILD_IF(OS_EMSCRIPTEN)

SRCS(
    ysafeptr_ut.cpp
    ysaveload_ut.cpp
)

END()
