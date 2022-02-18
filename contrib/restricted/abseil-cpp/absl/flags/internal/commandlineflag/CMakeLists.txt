add_library(flags-internal-commandlineflag)
target_compile_options(flags-internal-commandlineflag PRIVATE
  -DNOMINMAX
)
target_include_directories(flags-internal-commandlineflag PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_include_directories(flags-internal-commandlineflag PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp
)
target_link_libraries(flags-internal-commandlineflag PUBLIC
  contrib-libs-cxxsupp
)
target_sources(flags-internal-commandlineflag PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/abseil-cpp/absl/flags/internal/commandlineflag.cc
)
