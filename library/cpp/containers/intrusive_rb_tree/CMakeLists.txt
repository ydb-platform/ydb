add_library(cpp-containers-intrusive_rb_tree)
target_link_libraries(cpp-containers-intrusive_rb_tree PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-containers-intrusive_rb_tree PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/containers/intrusive_rb_tree/rb_tree.cpp
)
