add_library(ystrings-internal-cordz_functions)
target_include_directories(ystrings-internal-cordz_functions PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(ystrings-internal-cordz_functions PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(ystrings-internal-cordz_functions PUBLIC
  contrib-libs-cxxsupp
  yutil
  ybase-internal-raw_logging
  yabsl-base-log_severity
  yprofiling-internal-exponential_biased
)
target_sources(ystrings-internal-cordz_functions PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/strings/internal/cordz_functions.cc
)
