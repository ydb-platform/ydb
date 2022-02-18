find_package(OpenSSL)

add_library(cpp-openssl-init)
target_link_libraries(cpp-openssl-init PUBLIC
  contrib-libs-cxxsupp
  yutil
  OpenSSL::OpenSSL
)
target_sources(cpp-openssl-init PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/openssl/init/init.cpp
)
