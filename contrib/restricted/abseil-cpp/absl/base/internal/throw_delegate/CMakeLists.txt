add_library(base-internal-throw_delegate)
target_compile_options(base-internal-throw_delegate PRIVATE
  -DNOMINMAX
)
target_include_directories(base-internal-throw_delegate PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(base-internal-throw_delegate PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(base-internal-throw_delegate PUBLIC
  contrib-libs-cxxsupp
  base-internal-raw_logging
  absl-base-log_severity
)
target_sources(base-internal-throw_delegate PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/base/internal/throw_delegate.cc
)
