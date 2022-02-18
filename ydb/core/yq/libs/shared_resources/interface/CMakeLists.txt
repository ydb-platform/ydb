add_library(libs-shared_resources-interface)
target_compile_options(libs-shared_resources-interface PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(libs-shared_resources-interface PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
)
target_sources(libs-shared_resources-interface PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/shared_resources/interface/shared_resources.cpp
)
