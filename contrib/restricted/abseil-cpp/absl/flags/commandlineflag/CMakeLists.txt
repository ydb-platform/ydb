add_library(absl-flags-commandlineflag)
target_compile_options(absl-flags-commandlineflag PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-flags-commandlineflag PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-flags-commandlineflag PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-flags-commandlineflag PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  flags-internal-commandlineflag
  abseil-cpp-absl-numeric
  abseil-cpp-absl-strings
  strings-internal-absl_strings_internal
  absl-types-bad_optional_access
)
target_sources(absl-flags-commandlineflag PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/flags/commandlineflag.cc
)
