add_library(udf-service-exception_policy)
target_compile_options(udf-service-exception_policy PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(udf-service-exception_policy PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-minikql
  yql-public-udf
)
target_sources(udf-service-exception_policy PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/udf/service/exception_policy/udf_service.cpp
)
