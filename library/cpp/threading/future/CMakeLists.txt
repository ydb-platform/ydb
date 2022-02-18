add_library(cpp-threading-future)
target_link_libraries(cpp-threading-future PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-threading-future PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/future/async.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/future/core/future.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/future/core/fwd.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/future/fwd.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/future/wait/fwd.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/future/wait/wait.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/future/wait/wait_group.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/future/wait/wait_policy.cpp
)
