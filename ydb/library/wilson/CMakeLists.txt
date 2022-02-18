add_library(ydb-library-wilson INTERFACE)
target_link_libraries(ydb-library-wilson INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-actors-wilson
)
