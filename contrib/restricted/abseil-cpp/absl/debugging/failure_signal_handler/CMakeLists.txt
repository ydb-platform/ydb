add_library(absl-debugging-failure_signal_handler)
target_compile_options(absl-debugging-failure_signal_handler PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-debugging-failure_signal_handler PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-debugging-failure_signal_handler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-debugging-failure_signal_handler PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  abseil-cpp-absl-debugging
  absl-debugging-internal
  absl-debugging-stacktrace
  absl-debugging-symbolize
  abseil-cpp-absl-demangle
  abseil-cpp-absl-numeric
  abseil-cpp-absl-strings
  strings-internal-absl_strings_internal
)
target_sources(absl-debugging-failure_signal_handler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/debugging/failure_signal_handler.cc
)
