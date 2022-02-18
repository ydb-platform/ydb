add_library(messagebus_actor)
target_link_libraries(messagebus_actor PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(messagebus_actor PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/actor/executor.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/actor/thread_extra.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/actor/what_thread_does.cpp
)
