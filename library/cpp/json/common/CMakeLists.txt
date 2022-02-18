add_library(cpp-json-common)
target_link_libraries(cpp-json-common PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(cpp-json-common PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/json/common/defs.cpp
)
