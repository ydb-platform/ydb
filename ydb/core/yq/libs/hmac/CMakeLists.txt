find_package(OpenSSL REQUIRED)

add_library(yq-libs-hmac)
target_link_libraries(yq-libs-hmac PUBLIC
  contrib-libs-cxxsupp
  yutil
  OpenSSL::OpenSSL
  cpp-string_utils-base64
)
target_sources(yq-libs-hmac PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/hmac/hmac.cpp
)
