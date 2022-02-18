add_library(base-internal-raw_logging)
target_include_directories(base-internal-raw_logging PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(base-internal-raw_logging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(base-internal-raw_logging PUBLIC
  contrib-libs-cxxsupp
  yutil
  y_absl-base-log_severity
)
target_sources(base-internal-raw_logging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/raw_logging.cc
)
