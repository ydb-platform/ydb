add_library(abseil-cpp-absl-numeric)
target_compile_options(abseil-cpp-absl-numeric PRIVATE
  -DNOMINMAX
)
target_include_directories(abseil-cpp-absl-numeric PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(abseil-cpp-absl-numeric PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(abseil-cpp-absl-numeric PUBLIC
  contrib-libs-cxxsupp
)
target_sources(abseil-cpp-absl-numeric PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/numeric/int128.cc
)
