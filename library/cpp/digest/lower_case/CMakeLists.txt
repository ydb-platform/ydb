add_library(cpp-digest-lower_case)
target_link_libraries(cpp-digest-lower_case PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-digest-lower_case PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/lower_case/lciter.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/lower_case/lchash.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/digest/lower_case/hash_ops.cpp
)
