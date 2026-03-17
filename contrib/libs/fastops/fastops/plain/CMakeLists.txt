set(FMATH_DIR "${PROJECT_SOURCE_DIR}/contrib/libs/fmath")
set(FMATH_INCLUDE_DIR "${FMATH_DIR}")

include_directories("${FMATH_INCLUDE_DIR}")

add_library(plain ops_plain.cpp)
