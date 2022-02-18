add_library(yql-dq-common)
target_link_libraries(yql-dq-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  library-mkql_proto-protos
)
target_sources(yql-dq-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/common/dq_common.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/dq/common/dq_value.cpp
)
