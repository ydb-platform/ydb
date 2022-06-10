
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(y_absl-debugging-stacktrace)
target_include_directories(y_absl-debugging-stacktrace PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(y_absl-debugging-stacktrace PUBLIC
  contrib-libs-cxxsupp
  yutil
  base-internal-raw_logging
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-debugging
)
target_sources(y_absl-debugging-stacktrace PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/debugging/stacktrace.cc
)
