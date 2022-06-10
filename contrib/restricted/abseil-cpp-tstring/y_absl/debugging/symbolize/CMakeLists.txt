
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(y_absl-debugging-symbolize)
target_include_directories(y_absl-debugging-symbolize PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(y_absl-debugging-symbolize PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-debugging
  abseil-cpp-tstring-y_absl-demangle
  abseil-cpp-tstring-y_absl-numeric
  abseil-cpp-tstring-y_absl-strings
  strings-internal-absl_strings_internal
)
target_sources(y_absl-debugging-symbolize PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/debugging/symbolize.cc
)
