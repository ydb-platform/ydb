add_library(base-internal-scoped_set_env)
target_compile_options(base-internal-scoped_set_env PRIVATE
  -DNOMINMAX
)
target_include_directories(base-internal-scoped_set_env PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(base-internal-scoped_set_env PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(base-internal-scoped_set_env PUBLIC
  contrib-libs-cxxsupp
  base-internal-raw_logging
  absl-base-log_severity
)
target_sources(base-internal-scoped_set_env PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/base/internal/scoped_set_env.cc
)
