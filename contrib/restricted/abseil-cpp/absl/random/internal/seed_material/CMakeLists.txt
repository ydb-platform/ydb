add_library(random-internal-seed_material)
target_compile_options(random-internal-seed_material PRIVATE
  -DNOMINMAX
)
target_include_directories(random-internal-seed_material PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(random-internal-seed_material PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(random-internal-seed_material PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  abseil-cpp-absl-numeric
  abseil-cpp-absl-strings
  strings-internal-absl_strings_internal
  absl-types-bad_optional_access
)
target_sources(random-internal-seed_material PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/random/internal/seed_material.cc
)
