add_library(abseil-cpp-absl-demangle)
target_compile_options(abseil-cpp-absl-demangle PRIVATE
  -DNOMINMAX
)
target_include_directories(abseil-cpp-absl-demangle PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(abseil-cpp-absl-demangle PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(abseil-cpp-absl-demangle PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  absl-base-log_severity
)
target_sources(abseil-cpp-absl-demangle PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/debugging/internal/demangle.cc
)
