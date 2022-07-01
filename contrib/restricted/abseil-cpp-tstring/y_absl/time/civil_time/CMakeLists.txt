
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yabsl-time-civil_time)
target_compile_options(yabsl-time-civil_time PRIVATE
  -Wno-everything
)
target_include_directories(yabsl-time-civil_time PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-time-civil_time PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yabsl-time-civil_time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/civil_time_detail.cc
)
