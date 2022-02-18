add_library(certs INTERFACE)
target_link_libraries(certs INTERFACE
  contrib-libs-cxxsupp
  yutil
  library-cpp-resource
)

add_global_library_for(certs.global certs)
target_sources(certs.global PRIVATE
  ${CMAKE_BINARY_DIR}/certs/b03c08f91a00a60ac9d7daef45f3075a.cpp
)
resources(certs.global
  ${CMAKE_BINARY_DIR}/certs/b03c08f91a00a60ac9d7daef45f3075a.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/certs/cacert.pem
  KEYS
  /builtin/cacert
)
