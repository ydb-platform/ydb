find_package(OpenSSL REQUIRED)

add_library(library-http_proxy-authorization)
target_link_libraries(library-http_proxy-authorization PUBLIC
  contrib-libs-cxxsupp
  yutil
  OpenSSL::OpenSSL
  library-cpp-cgiparam
  cpp-http-io
  cpp-http-misc
  library-http_proxy-error
)
target_sources(library-http_proxy-authorization PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/http_proxy/authorization/auth_helpers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/http_proxy/authorization/signature.cpp
)
