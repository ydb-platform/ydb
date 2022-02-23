add_library(yabseil-cpp-tstring-y_absl-base)
target_include_directories(yabseil-cpp-tstring-y_absl-base PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabseil-cpp-tstring-y_absl-base PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabseil-cpp-tstring-y_absl-base PUBLIC
  contrib-libs-cxxsupp
  yutil
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  yabsl-base-log_severity
)
target_sources(yabseil-cpp-tstring-y_absl-base PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/cycleclock.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/spinlock.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/sysinfo.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/thread_identity.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/unscaledcycleclock.cc
)
