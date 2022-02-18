add_library(cpp-lfalloc-dbg_info)
target_link_libraries(cpp-lfalloc-dbg_info PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-malloc-api
)
target_sources(cpp-lfalloc-dbg_info PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/lfalloc/dbg_info/dbg_info.cpp
)
