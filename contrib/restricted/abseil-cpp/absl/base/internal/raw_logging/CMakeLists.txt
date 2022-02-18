add_library(base-internal-raw_logging)
target_compile_options(base-internal-raw_logging PRIVATE
  -DNOMINMAX
)
target_include_directories(base-internal-raw_logging PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(base-internal-raw_logging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(base-internal-raw_logging PUBLIC
  contrib-libs-cxxsupp
  absl-base-log_severity
)
target_sources(base-internal-raw_logging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/base/internal/raw_logging.cc
)
