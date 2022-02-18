add_library(absl-debugging-leak_check)
target_compile_options(absl-debugging-leak_check PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-debugging-leak_check PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-debugging-leak_check PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-debugging-leak_check PUBLIC
  contrib-libs-cxxsupp
)
target_sources(absl-debugging-leak_check PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/debugging/leak_check.cc
)
