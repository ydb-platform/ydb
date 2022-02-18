add_library(cpp-yson-json)
target_link_libraries(cpp-yson-json PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
)
target_sources(cpp-yson-json PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/json/json_writer.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/yson/json/yson2json_adapter.cpp
)
