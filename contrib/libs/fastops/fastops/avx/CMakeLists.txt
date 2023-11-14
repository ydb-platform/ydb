if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX /D__SSE4_1__=1 /D__SSE4_2__=1 -DNO_AVX2")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx -msse4 -msse4.1 -msse4.2 -Wno-unknown-pragmas -Wno-unused-local-typedef -DNO_AVX2")
endif()

add_library(avx ops_avx.cpp)
