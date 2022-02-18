add_library(base-internal-low_level_alloc)
target_include_directories(base-internal-low_level_alloc PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(base-internal-low_level_alloc PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(base-internal-low_level_alloc PUBLIC
  contrib-libs-cxxsupp
  yutil
  abseil-cpp-tstring-y_absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  y_absl-base-log_severity
)
target_sources(base-internal-low_level_alloc PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/low_level_alloc.cc
)
