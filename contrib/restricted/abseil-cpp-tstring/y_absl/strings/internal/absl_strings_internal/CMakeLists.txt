add_library(ystrings-internal-absl_strings_internal)
target_include_directories(ystrings-internal-absl_strings_internal PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(ystrings-internal-absl_strings_internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(ystrings-internal-absl_strings_internal PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  yabsl-base-log_severity
)
target_sources(ystrings-internal-absl_strings_internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/strings/internal/escaping.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/strings/internal/ostringstream.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/strings/internal/utf8.cc
)
