UNITTEST_FOR(util)

SUBSCRIBER(g:util-subscribers)

NO_BUILD_IF(OS_EMSCRIPTEN)

SRCS(
    ysaveload_ut.cpp
)

END()
