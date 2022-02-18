add_library(strings-internal-absl_strings_internal)
target_compile_options(strings-internal-absl_strings_internal PRIVATE
  -DNOMINMAX
)
target_include_directories(strings-internal-absl_strings_internal PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(strings-internal-absl_strings_internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(strings-internal-absl_strings_internal PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  absl-base-log_severity
)
target_sources(strings-internal-absl_strings_internal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/strings/internal/escaping.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/strings/internal/ostringstream.cc
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/strings/internal/utf8.cc
)
