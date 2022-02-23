add_library(yabsl-time-civil_time)
target_include_directories(yabsl-time-civil_time PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yabsl-time-civil_time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yabsl-time-civil_time PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yabsl-time-civil_time PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/time/internal/cctz/src/civil_time_detail.cc
)
