add_library(ydb-library-pdisk_io)
target_link_libraries(ydb-library-pdisk_io PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
  contrib-libs-libaio
  cpp-actors-core
  cpp-monlib-dynamic_counters
  ydb-core-debug
  library-pdisk_io-protos
  ydb-library-wilson
)
target_sources(ydb-library-pdisk_io PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/pdisk_io/aio_linux.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/pdisk_io/file_params_linux.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/pdisk_io/aio.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/pdisk_io/aio_map.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/pdisk_io/buffers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/pdisk_io/sector_map.cpp
)
generate_enum_serilization(ydb-library-pdisk_io
  ${CMAKE_SOURCE_DIR}/ydb/library/pdisk_io/aio.h
  INCLUDE_HEADERS
  ydb/library/pdisk_io/aio.h
)
