add_library(core-blobstorage-incrhuge)
target_link_libraries(core-blobstorage-incrhuge PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
)
target_sources(core-blobstorage-incrhuge PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_alloc.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_common.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_defrag.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_delete.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_log.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_read.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_recovery.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_recovery_read_log.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_recovery_scan.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/incrhuge/incrhuge_keeper_write.cpp
)
