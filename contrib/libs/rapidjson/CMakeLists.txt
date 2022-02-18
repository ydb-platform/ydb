add_library(contrib-libs-rapidjson INTERFACE)
target_link_libraries(contrib-libs-rapidjson INTERFACE
  contrib-libs-cxxsupp
  yutil
)
