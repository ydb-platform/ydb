add_library(core-protos-out)
target_link_libraries(core-protos-out PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-protos
)
target_sources(core-protos-out PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/protos/out/out.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/protos/out/out_sequenceshard.cpp
)
