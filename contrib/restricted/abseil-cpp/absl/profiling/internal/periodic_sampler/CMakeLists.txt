add_library(profiling-internal-periodic_sampler)
target_compile_options(profiling-internal-periodic_sampler PRIVATE
  -DNOMINMAX
)
target_include_directories(profiling-internal-periodic_sampler PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(profiling-internal-periodic_sampler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(profiling-internal-periodic_sampler PUBLIC
  contrib-libs-cxxsupp
  profiling-internal-exponential_biased
)
target_sources(profiling-internal-periodic_sampler PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/profiling/internal/periodic_sampler.cc
)
