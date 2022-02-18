add_library(abseil-cpp-absl-time)
target_compile_options(abseil-cpp-absl-time PRIVATE
  -DNOMINMAX
)
target_include_directories(abseil-cpp-absl-time PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(abseil-cpp-absl-time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(abseil-cpp-absl-time PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  abseil-cpp-absl-numeric
  abseil-cpp-absl-strings
  strings-internal-absl_strings_internal
  absl-time-civil_time
  absl-time-time_zone
)
target_sources(abseil-cpp-absl-time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/time/civil_time.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/time/clock.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/time/duration.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/time/format.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/time/time.cc
)
