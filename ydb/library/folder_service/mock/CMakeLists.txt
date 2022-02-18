add_library(library-folder_service-mock)
target_compile_options(library-folder_service-mock PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(library-folder_service-mock PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-library-folder_service
  library-folder_service-proto
)
target_sources(library-folder_service-mock PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/folder_service/mock/mock_folder_service.cpp
)
