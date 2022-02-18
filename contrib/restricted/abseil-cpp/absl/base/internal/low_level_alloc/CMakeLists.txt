add_library(base-internal-low_level_alloc)
target_compile_options(base-internal-low_level_alloc PRIVATE
  -DNOMINMAX
)
target_include_directories(base-internal-low_level_alloc PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(base-internal-low_level_alloc PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(base-internal-low_level_alloc PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  absl-base-log_severity
)
target_sources(base-internal-low_level_alloc PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/base/internal/low_level_alloc.cc
)
