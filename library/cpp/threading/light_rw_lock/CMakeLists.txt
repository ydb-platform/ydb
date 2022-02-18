add_library(cpp-threading-light_rw_lock)
target_link_libraries(cpp-threading-light_rw_lock PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-threading-light_rw_lock PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/light_rw_lock/lightrwlock.cpp
)
