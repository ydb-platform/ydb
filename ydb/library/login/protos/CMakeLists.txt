add_library(library-login-protos)
target_link_libraries(library-login-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(library-login-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/login/protos/login.proto
)
