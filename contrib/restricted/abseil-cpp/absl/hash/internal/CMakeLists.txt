add_library(absl-hash-internal)
target_compile_options(absl-hash-internal PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-hash-internal PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-hash-internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-hash-internal PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  absl-base-log_severity
  abseil-cpp-absl-numeric
)
target_sources(absl-hash-internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/hash/internal/low_level_hash.cc
)
