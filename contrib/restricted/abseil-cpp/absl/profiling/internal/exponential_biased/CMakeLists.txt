add_library(profiling-internal-exponential_biased)
target_compile_options(profiling-internal-exponential_biased PRIVATE
  -DNOMINMAX
)
target_include_directories(profiling-internal-exponential_biased PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(profiling-internal-exponential_biased PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(profiling-internal-exponential_biased PUBLIC
  contrib-libs-cxxsupp
)
target_sources(profiling-internal-exponential_biased PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/profiling/internal/exponential_biased.cc
)
