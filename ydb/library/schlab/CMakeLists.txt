add_library(ydb-library-schlab)
target_link_libraries(ydb-library-schlab PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-schlab-schine
)
target_sources(ydb-library-schlab PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/schlab_actor.cpp
)
