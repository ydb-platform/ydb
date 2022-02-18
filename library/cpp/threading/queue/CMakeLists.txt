add_library(cpp-threading-queue)
target_link_libraries(cpp-threading-queue PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-threading-queue PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/queue/mpmc_unordered_ring.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/queue/mpsc_htswap.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/queue/mpsc_intrusive_unordered.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/queue/mpsc_read_as_filled.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/threading/queue/mpsc_vinfarr_obstructive.cpp
)
