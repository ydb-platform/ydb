add_library(yabseil-cpp-tstring-y_absl-hash)
target_include_directories(yabseil-cpp-tstring-y_absl-hash PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabseil-cpp-tstring-y_absl-hash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabseil-cpp-tstring-y_absl-hash PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  ybase-internal-throw_delegate
  yabsl-base-log_severity
  yabseil-cpp-tstring-y_absl-city
  yabsl-hash-internal
  yabseil-cpp-tstring-y_absl-numeric
  yabseil-cpp-tstring-y_absl-strings
  ystrings-internal-absl_strings_internal
  yabsl-types-bad_optional_access
  yabsl-types-bad_variant_access
  abseil-cpp-tstring-y_absl-types
  y_absl-types-internal
)
target_sources(yabseil-cpp-tstring-y_absl-hash PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/hash/internal/hash.cc
)
