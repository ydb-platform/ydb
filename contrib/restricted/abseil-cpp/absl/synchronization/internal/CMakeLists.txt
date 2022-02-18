add_library(absl-synchronization-internal)
target_compile_options(absl-synchronization-internal PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-synchronization-internal PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-synchronization-internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-synchronization-internal PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-low_level_alloc
  base-internal-raw_logging
  base-internal-spinlock_wait
  absl-base-log_severity
)
target_sources(absl-synchronization-internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/synchronization/internal/graphcycles.cc
)
