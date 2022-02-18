add_library(base-internal-strerror)
target_compile_options(base-internal-strerror PRIVATE
  -DNOMINMAX
)
target_include_directories(base-internal-strerror PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(base-internal-strerror PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(base-internal-strerror PUBLIC
  contrib-libs-cxxsupp
)
target_sources(base-internal-strerror PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/base/internal/strerror.cc
)
