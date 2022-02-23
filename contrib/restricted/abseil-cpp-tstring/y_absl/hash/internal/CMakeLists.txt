add_library(yabsl-hash-internal)
target_include_directories(yabsl-hash-internal PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabsl-hash-internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-hash-internal PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  yabsl-base-log_severity
  yabseil-cpp-tstring-y_absl-numeric
)
target_sources(yabsl-hash-internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/hash/internal/low_level_hash.cc
)
