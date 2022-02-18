add_library(ydb-core-test_tablet)
target_link_libraries(ydb-core-test_tablet PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-digest-md5
  ydb-core-keyvalue
  ydb-core-protos
  ydb-core-util
)
target_sources(ydb-core-test_tablet PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/load_actor_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/load_actor_delete.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/load_actor_mon.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/load_actor_read_validate.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/load_actor_state.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/load_actor_write.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/state_server_interface.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/test_shard_mon.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/test_tablet.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/tx_init_scheme.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/tx_initialize.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/test_tablet/tx_load_everything.cpp
)
