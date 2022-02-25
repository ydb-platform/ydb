add_library(library-pdisk_io-protos)
target_link_libraries(library-pdisk_io-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(library-pdisk_io-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/pdisk_io/protos/sector_map.proto
)
