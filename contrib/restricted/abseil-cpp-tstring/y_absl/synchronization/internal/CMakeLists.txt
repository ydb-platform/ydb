
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(y_absl-synchronization-internal)
target_include_directories(y_absl-synchronization-internal PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(y_absl-synchronization-internal PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  y_absl-base-log_severity
)
target_sources(y_absl-synchronization-internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/synchronization/internal/graphcycles.cc
)
