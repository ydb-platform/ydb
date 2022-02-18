add_library(absl-random-seed_sequences)
target_compile_options(absl-random-seed_sequences PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-random-seed_sequences PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-random-seed_sequences PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-random-seed_sequences PUBLIC
  contrib-libs-cxxsupp
  abseil-cpp-absl-base
  base-internal-raw_logging
  base-internal-spinlock_wait
  base-internal-throw_delegate
  absl-base-log_severity
  abseil-cpp-absl-numeric
  random-internal-pool_urbg
  random-internal-randen
  random-internal-randen_detect
  random-internal-randen_hwaes
  random-internal-randen_round_keys
  random-internal-randen_slow
  random-internal-seed_material
  absl-random-seed_gen_exception
  abseil-cpp-absl-strings
  strings-internal-absl_strings_internal
  absl-types-bad_optional_access
)
target_sources(absl-random-seed_sequences PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/random/seed_sequences.cc
)
