add_library(cpp-containers-bitseq)
target_link_libraries(cpp-containers-bitseq PUBLIC
  contrib-libs-cxxsupp
  yutil
  util-draft
  library-cpp-pop_count
)
target_sources(cpp-containers-bitseq PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/bitseq/bitvector.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/bitseq/readonly_bitvector.cpp
)
