
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(ybase-internal-raw_logging)
target_include_directories(ybase-internal-raw_logging PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(ybase-internal-raw_logging PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabsl-base-log_severity
)
target_sources(ybase-internal-raw_logging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/raw_logging.cc
)
