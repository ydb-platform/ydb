add_library(contrib-libs-antlr3_cpp_runtime)
target_include_directories(contrib-libs-antlr3_cpp_runtime PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/antlr3_cpp_runtime/include
)
target_include_directories(contrib-libs-antlr3_cpp_runtime PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/antlr3_cpp_runtime/include
)
target_link_libraries(contrib-libs-antlr3_cpp_runtime PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(contrib-libs-antlr3_cpp_runtime PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/antlr3_cpp_runtime/antlr3.cpp
)
