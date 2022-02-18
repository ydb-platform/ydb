add_library(ydb-core-security)
target_link_libraries(ydb-core-security PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-actors-http
  monlib-service-pages
  cpp-openssl-io
  ydb-core-base
  ydb-core-protos
  ydb-library-aclib
  library-aclib-protos
  ydb-library-login
  ydb-library-security
)
target_sources(ydb-core-security PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/security/login_page.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/security/ticket_parser.cpp
)
