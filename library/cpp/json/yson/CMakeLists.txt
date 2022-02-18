add_library(cpp-json-yson)
target_link_libraries(cpp-json-yson PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
  library-cpp-yson
  cpp-yson-json
)
target_sources(cpp-json-yson PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/json/yson/json2yson.cpp
)
