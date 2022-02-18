add_library(yq-libs-config)
target_link_libraries(yq-libs-config PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-config-protos
  public-issue-protos
)
target_sources(yq-libs-config PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/config/yq_issue.cpp
)
