add_library(core-persqueue-config)
target_link_libraries(core-persqueue-config PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-protos
)
target_sources(core-persqueue-config PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/persqueue/config/config.cpp
)
