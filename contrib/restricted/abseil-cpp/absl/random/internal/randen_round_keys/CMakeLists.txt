add_library(random-internal-randen_round_keys)
target_compile_options(random-internal-randen_round_keys PRIVATE
  -DNOMINMAX
)
target_include_directories(random-internal-randen_round_keys PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(random-internal-randen_round_keys PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(random-internal-randen_round_keys PUBLIC
  contrib-libs-cxxsupp
)
target_sources(random-internal-randen_round_keys PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/random/internal/randen_round_keys.cc
)
