add_library(random-internal-randen)
target_compile_options(random-internal-randen PRIVATE
  -DNOMINMAX
)
target_include_directories(random-internal-randen PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(random-internal-randen PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(random-internal-randen PUBLIC
  contrib-libs-cxxsupp
  random-internal-randen_detect
  random-internal-randen_hwaes
  random-internal-randen_round_keys
  random-internal-randen_slow
)
target_sources(random-internal-randen PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/random/internal/randen.cc
)
