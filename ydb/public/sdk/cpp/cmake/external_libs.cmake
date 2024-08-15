find_package(IDN REQUIRED)
find_package(Iconv REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(ZLIB REQUIRED)
find_package(xxHash REQUIRED)
find_package(ZSTD REQUIRED)
find_package(BZip2 REQUIRED)
find_package(LZ4 REQUIRED)
find_package(Snappy 1.1.8 REQUIRED)
find_package(base64 REQUIRED)
find_package(Brotli 1.1.0 REQUIRED)
find_package(jwt-cpp REQUIRED)
find_package(GTest REQUIRED)
find_package(double-conversion REQUIRED)

# RapidJSON
if (YDB_SDK_USE_RAPID_JSON)
  find_package(RapidJSON REQUIRED)

  add_library(RapidJSON::RapidJSON INTERFACE IMPORTED)

  target_include_directories(RapidJSON::RapidJSON INTERFACE
    ${RAPIDJSON_INCLUDE_DIRS}
  )
endif()

# api-common-protos
if (YDB_SDK_GOOGLE_COMMON_PROTOS_TARGET)
  add_library(api-common-protos ALIAS ${YDB_SDK_GOOGLE_COMMON_PROTOS_TARGET})
else()
  file(MAKE_DIRECTORY ${YDB_SDK_BINARY_DIR}/third_party/api-common-protos)
  file(GLOB_RECURSE API_COMMON_PROTOS_SOURCES
    ${YDB_SDK_SOURCE_DIR}/third_party/api-common-protos/google/*.proto
  )

  _ydb_sdk_init_proto_library_impl(api-common-protos Off)

  set_property(TARGET api-common-protos PROPERTY
    PROTO_NAMESPACE third_party/api-common-protos
  )

  set_property(TARGET api-common-protos APPEND PROPERTY 
    PROTO_ADDINCL 
      ./third_party/api-common-protos
      ${YDB_SDK_SOURCE_DIR}
  )

  target_include_directories(api-common-protos PUBLIC
    $<BUILD_INTERFACE:${YDB_SDK_BINARY_DIR}/third_party/api-common-protos>
  )

  _ydb_sdk_gen_proto_messages(api-common-protos PRIVATE Off ${API_COMMON_PROTOS_SOURCES})

  _ydb_sdk_install_targets(TARGETS api-common-protos)
endif()

# FastLZ
add_library(FastLZ 
  ${YDB_SDK_SOURCE_DIR}/third_party/FastLZ/fastlz.c
)

target_include_directories(FastLZ PUBLIC
  $<BUILD_INTERFACE:${YDB_SDK_SOURCE_DIR}/third_party/FastLZ>
)

_ydb_sdk_install_targets(TARGETS FastLZ)

# nayuki_md5
add_library(nayuki_md5)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
target_sources(nayuki_md5 PRIVATE 
  ${YDB_SDK_SOURCE_DIR}/third_party/nayuki_md5/nayuki_md5-fast-x8664.S
)
else()
target_sources(nayuki_md5 PRIVATE 
  ${YDB_SDK_SOURCE_DIR}/third_party/nayuki_md5/nayuki_md5.c
)
endif()

target_include_directories(nayuki_md5 PUBLIC
  $<BUILD_INTERFACE:${YDB_SDK_SOURCE_DIR}/third_party/nayuki_md5>
  $<INSTALL_INTERFACE:third_party/nayuki_md5>
)

_ydb_sdk_install_targets(TARGETS nayuki_md5)
