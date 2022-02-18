add_library(providers-common-db_id_async_resolver INTERFACE)
target_link_libraries(providers-common-db_id_async_resolver INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-threading-future
  yql-public-issue
)
