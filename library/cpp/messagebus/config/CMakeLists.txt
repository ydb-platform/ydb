add_library(cpp-messagebus-config)
target_link_libraries(cpp-messagebus-config PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-getopt
  cpp-deprecated-enum_codegen
)
target_sources(cpp-messagebus-config PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/config/netaddr.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/config/session_config.cpp
)
