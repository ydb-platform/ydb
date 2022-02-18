add_library(abseil-cpp-tstring-y_absl-numeric)
target_include_directories(abseil-cpp-tstring-y_absl-numeric PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(abseil-cpp-tstring-y_absl-numeric PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(abseil-cpp-tstring-y_absl-numeric PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(abseil-cpp-tstring-y_absl-numeric PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/numeric/int128.cc
)
