
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yabsl-base-log_severity)
target_include_directories(yabsl-base-log_severity PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-base-log_severity PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yabsl-base-log_severity PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/log_severity.cc
)
