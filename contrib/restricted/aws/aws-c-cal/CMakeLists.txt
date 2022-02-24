find_package(OpenSSL REQUIRED)

add_library(restricted-aws-aws-c-cal)
target_include_directories(restricted-aws-aws-c-cal PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/include
)
target_include_directories(restricted-aws-aws-c-cal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/include
)
target_link_libraries(restricted-aws-aws-c-cal PUBLIC
  OpenSSL::OpenSSL
  restricted-aws-aws-c-common
)
target_sources(restricted-aws-aws-c-cal PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/cal.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/der.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/ecc.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/hash.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/hmac.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/unix/openssl_platform_init.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/unix/opensslcrypto_ecc.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/unix/opensslcrypto_hash.c
  ${CMAKE_SOURCE_DIR}/contrib/restricted/aws/aws-c-cal/source/unix/opensslcrypto_hmac.c
)
