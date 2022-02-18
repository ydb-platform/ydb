add_library(abseil-cpp-tstring-y_absl-status)
target_include_directories(abseil-cpp-tstring-y_absl-status PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(abseil-cpp-tstring-y_absl-status PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(abseil-cpp-tstring-y_absl-status PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-debugging
  y_absl-debugging-stacktrace
  y_absl-debugging-symbolize
  abseil-cpp-tstring-y_absl-demangle
  abseil-cpp-tstring-y_absl-numeric
  profiling-internal-exponential_biased
  abseil-cpp-tstring-y_absl-strings
  y_absl-strings-cord
  strings-internal-absl_cord_internal
  strings-internal-absl_strings_internal
  strings-internal-cordz_functions
  strings-internal-cordz_handle
  strings-internal-cordz_info
  strings-internal-str_format
  abseil-cpp-tstring-y_absl-synchronization
  y_absl-synchronization-internal
  abseil-cpp-tstring-y_absl-time
  y_absl-time-civil_time
  y_absl-time-time_zone
  y_absl-types-bad_optional_access
)
target_sources(abseil-cpp-tstring-y_absl-status PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/status/status.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/status/status_payload_printer.cc
)
