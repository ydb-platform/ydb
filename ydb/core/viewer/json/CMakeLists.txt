add_library(core-viewer-json)
target_link_libraries(core-viewer-json PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-string_utils-base64
  core-viewer-protos
)
target_sources(core-viewer-json PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/viewer/json/json.cpp
)
