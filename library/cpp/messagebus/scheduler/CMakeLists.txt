add_library(cpp-messagebus-scheduler)
target_link_libraries(cpp-messagebus-scheduler PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-threading-future
)
target_sources(cpp-messagebus-scheduler PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/scheduler/scheduler.cpp
)
