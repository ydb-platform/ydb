add_library(cpp-monlib-messagebus)
target_link_libraries(cpp-monlib-messagebus PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-messagebus
  cpp-messagebus-www
  cpp-monlib-dynamic_counters
)
target_sources(cpp-monlib-messagebus PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/messagebus/mon_messagebus.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/messagebus/mon_service_messagebus.cpp
)
