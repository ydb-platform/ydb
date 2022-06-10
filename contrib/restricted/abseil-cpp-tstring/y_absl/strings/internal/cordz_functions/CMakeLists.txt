
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(strings-internal-cordz_functions)
target_include_directories(strings-internal-cordz_functions PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(strings-internal-cordz_functions PUBLIC
  contrib-libs-cxxsupp
  yutil
  base-internal-raw_logging
  y_absl-base-log_severity
  profiling-internal-exponential_biased
)
target_sources(strings-internal-cordz_functions PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/strings/internal/cordz_functions.cc
)
