add_library(ybase-internal-raw_logging)
target_include_directories(ybase-internal-raw_logging PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(ybase-internal-raw_logging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(ybase-internal-raw_logging PUBLIC
  contrib-libs-cxxsupp
  yutil
  yabsl-base-log_severity
)
target_sources(ybase-internal-raw_logging PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/base/internal/raw_logging.cc
)
