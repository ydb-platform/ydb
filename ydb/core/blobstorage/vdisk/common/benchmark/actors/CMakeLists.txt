add_executable(actors_bench)
target_link_libraries(actors_bench PUBLIC
  contrib-libs-linux-headers
  contrib-libs-cxxsupp
  blobstorage-vdisk-common
  cpp-actors-core
)
target_link_options(corobenchmark PRIVATE
  -ldl
  -lrt
  -Wl,--no-as-needed
  -fPIC
  -fPIC
  -lpthread
  -lrt
  -ldl
)
target_sources(actors_bench PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/benchmark/actors/main.cpp
)
target_allocator(actors_bench
  cpp-malloc-tcmalloc
  libs-tcmalloc-no_percpu_cache
)
vcs_info(actors_bench)
