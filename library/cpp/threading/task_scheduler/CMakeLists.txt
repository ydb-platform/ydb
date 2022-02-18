add_library(cpp-threading-task_scheduler)
target_link_libraries(cpp-threading-task_scheduler PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-threading-task_scheduler PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/task_scheduler/task_scheduler.cpp
)
