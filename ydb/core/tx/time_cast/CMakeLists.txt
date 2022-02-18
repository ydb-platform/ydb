add_library(core-tx-time_cast)
target_link_libraries(core-tx-time_cast PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
  ydb-core-tablet
  ydb-core-tx
)
target_sources(core-tx-time_cast PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/time_cast/time_cast.cpp
)
