add_library(absl-debugging-stacktrace)
target_compile_options(absl-debugging-stacktrace PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-debugging-stacktrace PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-debugging-stacktrace PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-debugging-stacktrace PUBLIC
  contrib-libs-cxxsupp
  base-internal-raw_logging
  absl-base-log_severity
  abseil-cpp-absl-debugging
)
target_sources(absl-debugging-stacktrace PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/debugging/stacktrace.cc
)
