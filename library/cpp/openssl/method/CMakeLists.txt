add_library(cpp-openssl-method)
target_link_libraries(cpp-openssl-method PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-openssl-holders
)
target_sources(cpp-openssl-method PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/openssl/method/io.cpp
)
