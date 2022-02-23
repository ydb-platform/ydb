add_library(yprofiling-internal-exponential_biased)
target_include_directories(yprofiling-internal-exponential_biased PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_include_directories(yprofiling-internal-exponential_biased PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yprofiling-internal-exponential_biased PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yprofiling-internal-exponential_biased PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/profiling/internal/exponential_biased.cc
)
