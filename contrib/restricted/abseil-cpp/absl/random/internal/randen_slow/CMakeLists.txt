add_library(random-internal-randen_slow)
target_compile_options(random-internal-randen_slow PRIVATE
  -DNOMINMAX
)
target_include_directories(random-internal-randen_slow PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(random-internal-randen_slow PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(random-internal-randen_slow PUBLIC
  contrib-libs-cxxsupp
  random-internal-randen_round_keys
)
target_sources(random-internal-randen_slow PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/random/internal/randen_slow.cc
)
