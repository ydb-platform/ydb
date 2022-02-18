add_library(monlib-deprecated-json)
target_link_libraries(monlib-deprecated-json PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-json
)
target_sources(monlib-deprecated-json PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/monlib/deprecated/json/writer.cpp
)
