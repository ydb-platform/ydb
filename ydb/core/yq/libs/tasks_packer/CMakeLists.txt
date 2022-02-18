add_library(yq-libs-tasks_packer)
target_compile_options(yq-libs-tasks_packer PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-tasks_packer PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-dq-proto
  library-yql-utils
)
target_sources(yq-libs-tasks_packer PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/tasks_packer/tasks_packer.cpp
)
