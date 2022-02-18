add_library(absl-flags-marshalling)
target_compile_options(absl-flags-marshalling PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-flags-marshalling PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-flags-marshalling PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-flags-marshalling PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  abseil-cpp-absl-numeric
  abseil-cpp-absl-strings
  strings-internal-absl_strings_internal
  strings-internal-str_format
)
target_sources(absl-flags-marshalling PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/flags/marshalling.cc
)
