add_library(api-protos-out)
target_link_libraries(api-protos-out PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
)
target_sources(api-protos-out PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/api/protos/out/out.cpp
)
