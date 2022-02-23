add_library(yabseil-cpp-tstring-y_absl-demangle)
target_include_directories(yabseil-cpp-tstring-y_absl-demangle PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabseil-cpp-tstring-y_absl-demangle PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabseil-cpp-tstring-y_absl-demangle PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  yabsl-base-log_severity
)
target_sources(yabseil-cpp-tstring-y_absl-demangle PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/debugging/internal/demangle.cc
)
