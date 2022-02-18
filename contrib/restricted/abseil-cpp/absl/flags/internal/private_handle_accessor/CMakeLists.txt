add_library(flags-internal-private_handle_accessor)
target_compile_options(flags-internal-private_handle_accessor PRIVATE
  -DNOMINMAX
)
target_include_directories(flags-internal-private_handle_accessor PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(flags-internal-private_handle_accessor PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(flags-internal-private_handle_accessor PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  absl-flags-commandlineflag
  flags-internal-commandlineflag
  abseil-cpp-absl-numeric
  abseil-cpp-absl-strings
  strings-internal-absl_strings_internal
  absl-types-bad_optional_access
)
target_sources(flags-internal-private_handle_accessor PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/flags/internal/private_handle_accessor.cc
)
