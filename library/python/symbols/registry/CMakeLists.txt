add_library(python-symbols-registry)
target_link_libraries(python-symbols-registry PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(python-symbols-registry PRIVATE
  ${CMAKE_SOURCE_DIR}/library/python/symbols/registry/syms.cpp
)
