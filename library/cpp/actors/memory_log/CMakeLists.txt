add_library(cpp-actors-memory_log)
target_link_libraries(cpp-actors-memory_log PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-threading-queue
  contrib-libs-linuxvdso
)
target_sources(cpp-actors-memory_log PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/memory_log/memlog.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/memory_log/mmap.cpp
)
