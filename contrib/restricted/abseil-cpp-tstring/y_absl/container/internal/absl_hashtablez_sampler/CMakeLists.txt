
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(container-internal-absl_hashtablez_sampler)
target_include_directories(container-internal-absl_hashtablez_sampler PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(container-internal-absl_hashtablez_sampler PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  y_absl-base-log_severity
  abseil-cpp-tstring-y_absl-debugging
  y_absl-debugging-stacktrace
  y_absl-debugging-symbolize
  abseil-cpp-tstring-y_absl-demangle
  abseil-cpp-tstring-y_absl-numeric
  profiling-internal-exponential_biased
  abseil-cpp-tstring-y_absl-strings
  strings-internal-absl_strings_internal
  abseil-cpp-tstring-y_absl-synchronization
  y_absl-synchronization-internal
  abseil-cpp-tstring-y_absl-time
  y_absl-time-civil_time
  y_absl-time-time_zone
)
target_sources(container-internal-absl_hashtablez_sampler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/container/internal/hashtablez_sampler.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/container/internal/hashtablez_sampler_force_weak_definition.cc
)
