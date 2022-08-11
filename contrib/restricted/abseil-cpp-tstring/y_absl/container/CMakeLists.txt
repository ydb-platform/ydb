
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(abseil-cpp-tstring-y_absl-container)
target_compile_options(abseil-cpp-tstring-y_absl-container PRIVATE
  -Wno-everything
)
target_include_directories(abseil-cpp-tstring-y_absl-container PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(abseil-cpp-tstring-y_absl-container PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  abseil-cpp-tstring-y_absl-debugging
  abseil-cpp-tstring-y_absl-demangle
  abseil-cpp-tstring-y_absl-hash
  abseil-cpp-tstring-y_absl-numeric
  abseil-cpp-tstring-y_absl-profiling
  abseil-cpp-tstring-y_absl-strings
  abseil-cpp-tstring-y_absl-synchronization
  abseil-cpp-tstring-y_absl-time
  abseil-cpp-tstring-y_absl-types
)
target_sources(abseil-cpp-tstring-y_absl-container PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/container/internal/hashtablez_sampler.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/container/internal/hashtablez_sampler_force_weak_definition.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/container/internal/raw_hash_set.cc
)
