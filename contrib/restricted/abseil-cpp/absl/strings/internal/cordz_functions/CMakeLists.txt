add_library(strings-internal-cordz_functions)
target_compile_options(strings-internal-cordz_functions PRIVATE
  -DNOMINMAX
)
target_include_directories(strings-internal-cordz_functions PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(strings-internal-cordz_functions PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(strings-internal-cordz_functions PUBLIC
  contrib-libs-cxxsupp
  base-internal-raw_logging
  absl-base-log_severity
  profiling-internal-exponential_biased
)
target_sources(strings-internal-cordz_functions PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/strings/internal/cordz_functions.cc
)
