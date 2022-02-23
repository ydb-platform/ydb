add_library(ybase-internal-throw_delegate)
target_include_directories(ybase-internal-throw_delegate PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(ybase-internal-throw_delegate PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(ybase-internal-throw_delegate PUBLIC
  contrib-libs-cxxsupp
  yutil
  ybase-internal-raw_logging
  yabsl-base-log_severity
)
target_sources(ybase-internal-throw_delegate PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/throw_delegate.cc
)
