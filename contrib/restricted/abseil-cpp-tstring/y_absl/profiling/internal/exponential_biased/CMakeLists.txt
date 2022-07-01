
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yprofiling-internal-exponential_biased)
target_compile_options(yprofiling-internal-exponential_biased PRIVATE
  -Wno-everything
)
target_include_directories(yprofiling-internal-exponential_biased PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring
)
target_link_libraries(yprofiling-internal-exponential_biased PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yprofiling-internal-exponential_biased PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp-tstring/y_absl/profiling/internal/exponential_biased.cc
)
