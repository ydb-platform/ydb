add_library(ydb-library-dynumber)
target_link_libraries(ydb-library-dynumber PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-containers-stack_vector
)
target_sources(ydb-library-dynumber PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/dynumber/dynumber.cpp
)
