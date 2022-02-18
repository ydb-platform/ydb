add_library(services-lib-sharding)
target_link_libraries(services-lib-sharding PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(services-lib-sharding PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/lib/sharding/sharding.cpp
)
