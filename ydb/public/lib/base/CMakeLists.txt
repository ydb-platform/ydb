add_library(public-lib-base)
target_link_libraries(public-lib-base PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-deprecated-enum_codegen
  library-cpp-messagebus
  messagebus_protobuf
  ydb-core-protos
)
target_sources(public-lib-base PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/base/msgbus.cpp
)
