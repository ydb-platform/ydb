add_library(y_absl-time-civil_time)
target_include_directories(y_absl-time-civil_time PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(y_absl-time-civil_time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(y_absl-time-civil_time PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(y_absl-time-civil_time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/civil_time_detail.cc
)
