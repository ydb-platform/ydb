add_library(core-persqueue-codecs)
target_link_libraries(core-persqueue-codecs PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
  cpp-client-ydb_persqueue_core
)
target_sources(core-persqueue-codecs PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/persqueue/codecs/pqv1.cpp
)
