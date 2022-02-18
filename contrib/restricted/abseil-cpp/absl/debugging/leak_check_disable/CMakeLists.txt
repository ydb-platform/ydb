add_library(absl-debugging-leak_check_disable)
target_compile_options(absl-debugging-leak_check_disable PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-debugging-leak_check_disable PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-debugging-leak_check_disable PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-debugging-leak_check_disable PUBLIC
  contrib-libs-cxxsupp
)
target_sources(absl-debugging-leak_check_disable PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/debugging/leak_check_disable.cc
)
