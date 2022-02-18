add_library(y_absl-base-log_severity)
target_include_directories(y_absl-base-log_severity PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(y_absl-base-log_severity PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(y_absl-base-log_severity PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(y_absl-base-log_severity PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/log_severity.cc
)
