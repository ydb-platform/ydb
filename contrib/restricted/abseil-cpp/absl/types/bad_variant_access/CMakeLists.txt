add_library(absl-types-bad_variant_access)
target_compile_options(absl-types-bad_variant_access PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-types-bad_variant_access PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-types-bad_variant_access PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-types-bad_variant_access PUBLIC
  contrib-libs-cxxsupp
  base-internal-raw_logging
  absl-base-log_severity
)
target_sources(absl-types-bad_variant_access PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/types/bad_variant_access.cc
)
