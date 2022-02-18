add_library(abseil-cpp-tstring-y_absl-hash)
target_include_directories(abseil-cpp-tstring-y_absl-hash PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(abseil-cpp-tstring-y_absl-hash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(abseil-cpp-tstring-y_absl-hash PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-city
  y_absl-hash-internal
  abseil-cpp-tstring-y_absl-numeric
  abseil-cpp-tstring-y_absl-strings
  strings-internal-absl_strings_internal
  y_absl-types-bad_optional_access
  y_absl-types-bad_variant_access
  abseil-cpp-tstring-y_absl-types
  y_absl-types-internal
)
target_sources(abseil-cpp-tstring-y_absl-hash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/hash/internal/hash.cc
)
