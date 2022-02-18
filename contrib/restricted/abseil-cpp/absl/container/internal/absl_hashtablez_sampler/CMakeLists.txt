add_library(container-internal-absl_hashtablez_sampler)
target_compile_options(container-internal-absl_hashtablez_sampler PRIVATE
  -DNOMINMAX
)
target_include_directories(container-internal-absl_hashtablez_sampler PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(container-internal-absl_hashtablez_sampler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(container-internal-absl_hashtablez_sampler PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  abseil-cpp-absl-debugging
  absl-debugging-stacktrace
  absl-debugging-symbolize
  abseil-cpp-absl-demangle
  abseil-cpp-absl-numeric
  profiling-internal-exponential_biased
  abseil-cpp-absl-strings
  strings-internal-absl_strings_internal
  abseil-cpp-absl-synchronization
  absl-synchronization-internal
  abseil-cpp-absl-time
  absl-time-civil_time
  absl-time-time_zone
)
target_sources(container-internal-absl_hashtablez_sampler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/container/internal/hashtablez_sampler.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/container/internal/hashtablez_sampler_force_weak_definition.cc
)
