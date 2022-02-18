add_library(library-schlab-probes)
target_link_libraries(library-schlab-probes PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lwtrace
)
target_sources(library-schlab-probes PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/probes/probes.cpp
)
