add_library(blobstorage-vdisk-common)
target_link_libraries(blobstorage-vdisk-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-protos
  monlib-service-pages
  ydb-core-base
  vdisk-hulldb-base
  ydb-core-protos
)
target_sources(blobstorage-vdisk-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/blobstorage_event_filter.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/blobstorage_status.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/blobstorage_vdisk_guids.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_config.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_context.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_costmodel.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_events.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_handle_class.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_histogram_latency.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_histograms.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_log.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_outofspace.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_recoverylogwriter.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/vdisk_response.cpp
)
