add_library(yabsl-debugging-stacktrace)
target_include_directories(yabsl-debugging-stacktrace PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabsl-debugging-stacktrace PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-debugging-stacktrace PUBLIC
  contrib-libs-cxxsupp
  yutil
  ybase-internal-raw_logging
  yabsl-base-log_severity
  yabseil-cpp-tstring-y_absl-debugging
)
target_sources(yabsl-debugging-stacktrace PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/debugging/stacktrace.cc
)
