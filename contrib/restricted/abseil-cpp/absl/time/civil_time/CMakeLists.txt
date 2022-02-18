add_library(absl-time-civil_time)
target_compile_options(absl-time-civil_time PRIVATE
  -DNOMINMAX
)
target_include_directories(absl-time-civil_time PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(absl-time-civil_time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(absl-time-civil_time PUBLIC
  contrib-libs-cxxsupp
)
target_sources(absl-time-civil_time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/time/internal/cctz/src/civil_time_detail.cc
)
