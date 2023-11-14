if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2 /D__SSE4_1__=1 /D__SSE4_2__=1")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx -mavx2 -mfma -msse4 -msse4.1 -msse4.2 -Wno-unknown-pragmas -Wno-unused-local-typedef")
endif()

add_library(avx2 ops_avx2.cpp)
