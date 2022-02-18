add_library(cpp-deprecated-enum_codegen)
target_link_libraries(cpp-deprecated-enum_codegen PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-deprecated-enum_codegen PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/deprecated/enum_codegen/enum_codegen.cpp
)
