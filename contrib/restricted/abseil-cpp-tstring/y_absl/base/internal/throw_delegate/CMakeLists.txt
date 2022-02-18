add_library(base-internal-throw_delegate)
target_include_directories(base-internal-throw_delegate PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(base-internal-throw_delegate PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(base-internal-throw_delegate PUBLIC
  contrib-libs-cxxsupp
  yutil
  base-internal-raw_logging
  y_absl-base-log_severity
)
target_sources(base-internal-throw_delegate PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/throw_delegate.cc
)
