add_executable(vdisk_bench)
target_link_libraries(vdisk_bench PUBLIC
  contrib-libs-linux-headers
  testing-benchmark-main
  blobstorage-vdisk-common
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
target_sources(vdisk_bench PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/common/benchmark/proto/main.cpp
)
target_allocator(vdisk_bench
  cpp-malloc-tcmalloc
  libs-tcmalloc-no_percpu_cache
)
vcs_info(vdisk_bench)
