add_library(ydb-library-login)
target_link_libraries(ydb-library-login PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-jwt-cpp
  contrib-libs-protobuf
  cpp-digest-argonish
  library-cpp-json
  cpp-string_utils-base64
  library-login-protos
)
target_sources(ydb-library-login PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/login/login.cpp
)
