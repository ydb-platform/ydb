add_library(digest_udf INTERFACE)
target_link_libraries(digest_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  contrib-libs-farmhash
  contrib-libs-highwayhash
  contrib-libs-xxhash
  cpp-digest-argonish
  cpp-digest-crc32c
  cpp-digest-md5
  cpp-digest-old_crc
  cpp-digest-sfh
)

add_global_library_for(digest_udf.global digest_udf)
target_compile_options(digest_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_include_directories(digest_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash
)
target_link_libraries(digest_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  contrib-libs-farmhash
  contrib-libs-highwayhash
  contrib-libs-xxhash
  cpp-digest-argonish
  cpp-digest-crc32c
  cpp-digest-md5
  cpp-digest-old_crc
  cpp-digest-sfh
)
target_sources(digest_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/digest/digest_udf.cpp
)

add_library(digest_udf.udf SHARED)
target_compile_options(digest_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_include_directories(digest_udf.udf PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/highwayhash
)
target_link_libraries(digest_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  contrib-libs-farmhash
  contrib-libs-highwayhash
  contrib-libs-xxhash
  cpp-digest-argonish
  cpp-digest-crc32c
  cpp-digest-md5
  cpp-digest-old_crc
  cpp-digest-sfh
)
