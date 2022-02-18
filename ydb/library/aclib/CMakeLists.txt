add_library(ydb-library-aclib)
target_link_libraries(ydb-library-aclib PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-protobuf-util
  library-aclib-protos
)
target_sources(ydb-library-aclib PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/aclib/aclib.cpp
)
