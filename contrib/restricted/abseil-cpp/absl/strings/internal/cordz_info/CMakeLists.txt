add_library(strings-internal-cordz_info)
target_compile_options(strings-internal-cordz_info PRIVATE
  -DNOMINMAX
)
target_include_directories(strings-internal-cordz_info PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(strings-internal-cordz_info PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(strings-internal-cordz_info PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  abseil-cpp-absl-debugging
  absl-debugging-stacktrace
  absl-debugging-symbolize
  abseil-cpp-absl-demangle
  abseil-cpp-absl-numeric
  profiling-internal-exponential_biased
  abseil-cpp-absl-strings
  strings-internal-absl_cord_internal
  strings-internal-absl_strings_internal
  strings-internal-cordz_functions
  strings-internal-cordz_handle
  abseil-cpp-absl-synchronization
  absl-synchronization-internal
  abseil-cpp-absl-time
  absl-time-civil_time
  absl-time-time_zone
)
target_sources(strings-internal-cordz_info PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/strings/internal/cordz_info.cc
)
