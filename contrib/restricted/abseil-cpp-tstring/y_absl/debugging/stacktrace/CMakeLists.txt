add_library(y_absl-debugging-stacktrace)
target_include_directories(y_absl-debugging-stacktrace PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(y_absl-debugging-stacktrace PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(y_absl-debugging-stacktrace PUBLIC
  contrib-libs-cxxsupp
  yutil
  base-internal-raw_logging
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-debugging
)
target_sources(y_absl-debugging-stacktrace PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/debugging/stacktrace.cc
)
