add_library(absl-random-seed_gen_exception)
target_compile_options(absl-random-seed_gen_exception PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-random-seed_gen_exception PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-random-seed_gen_exception PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-random-seed_gen_exception PUBLIC
  contrib-libs-cxxsupp
)
target_sources(absl-random-seed_gen_exception PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/random/seed_gen_exception.cc
)
