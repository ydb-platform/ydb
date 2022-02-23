add_library(yabsl-debugging-symbolize)
target_include_directories(yabsl-debugging-symbolize PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabsl-debugging-symbolize PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-debugging-symbolize PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabseil-cpp-tstring-y_absl-base
  ybase-internal-low_level_alloc
  ybase-internal-raw_logging
  ybase-internal-spinlock_wait
  ybase-internal-throw_delegate
  yabsl-base-log_severity
  yabseil-cpp-tstring-y_absl-debugging
  yabseil-cpp-tstring-y_absl-demangle
  yabseil-cpp-tstring-y_absl-numeric
  yabseil-cpp-tstring-y_absl-strings
  ystrings-internal-absl_strings_internal
)
target_sources(yabsl-debugging-symbolize PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/debugging/symbolize.cc
)
