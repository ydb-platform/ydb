
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yabsl-synchronization-internal)
target_include_directories(yabsl-synchronization-internal PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-synchronization-internal PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-low_level_alloc
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  yabsl-base-log_severity
)
target_sources(yabsl-synchronization-internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/internal/graphcycles.cc
)
