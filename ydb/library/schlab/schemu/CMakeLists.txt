add_library(library-schlab-schemu)
target_link_libraries(library-schlab-schemu PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-schlab-schine
  library-schlab-schoot
)
target_sources(library-schlab-schemu PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schemu/schemu.cpp
)
