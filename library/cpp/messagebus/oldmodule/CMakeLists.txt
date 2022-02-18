add_library(cpp-messagebus-oldmodule)
target_link_libraries(cpp-messagebus-oldmodule PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-messagebus
  messagebus_actor
)
target_sources(cpp-messagebus-oldmodule PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/oldmodule/module.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/oldmodule/startsession.cpp
)
