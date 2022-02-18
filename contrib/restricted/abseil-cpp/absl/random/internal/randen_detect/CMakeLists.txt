add_library(random-internal-randen_detect)
target_compile_options(random-internal-randen_detect PRIVATE
  -DNOMINMAX
)
target_include_directories(random-internal-randen_detect PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(random-internal-randen_detect PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(random-internal-randen_detect PUBLIC
  contrib-libs-cxxsupp
  random-internal-randen_hwaes
  random-internal-randen_round_keys
)
target_sources(random-internal-randen_detect PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/random/internal/randen_detect.cc
)
