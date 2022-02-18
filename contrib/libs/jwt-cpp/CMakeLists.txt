find_package(OpenSSL)

add_library(contrib-libs-jwt-cpp INTERFACE)
target_link_libraries(contrib-libs-jwt-cpp INTERFACE
  OpenSSL::OpenSSL
)
