add_library(yabsl-types-bad_variant_access)
target_include_directories(yabsl-types-bad_variant_access PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabsl-types-bad_variant_access PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-types-bad_variant_access PUBLIC
  contrib-libs-cxxsupp
  yutil
  ybase-internal-raw_logging
  yabsl-base-log_severity
)
target_sources(yabsl-types-bad_variant_access PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/types/bad_variant_access.cc
)
