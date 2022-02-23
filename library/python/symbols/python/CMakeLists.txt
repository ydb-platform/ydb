add_library(python-symbols-python INTERFACE)
target_link_libraries(python-symbols-python INTERFACE
  contrib-libs-cxxsupp
  yutil
  python-symbols-registry
)

add_global_library_for(python-symbols-python.global python-symbols-python)
target_link_libraries(python-symbols-python.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  python-symbols-registry
)
target_sources(python-symbols-python.global PRIVATE
  ${CMAKE_SOURCE_DIR}/library/python/symbols/python/syms.cpp
)
