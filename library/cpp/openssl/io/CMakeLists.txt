add_library(cpp-openssl-io)
target_link_libraries(cpp-openssl-io PUBLIC
  contrib-libs-cxxsupp
  yutil
  certs
  cpp-openssl-init
  cpp-openssl-method
)
target_sources(cpp-openssl-io PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/openssl/io/stream.cpp
)
