add_library(library-persqueue-counter_time_keeper)
target_link_libraries(library-persqueue-counter_time_keeper PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-protos
  ydb-core-protos
)
target_sources(library-persqueue-counter_time_keeper PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/persqueue/counter_time_keeper/counter_time_keeper.cpp
)
