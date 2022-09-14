
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(libs-apache-avro)
target_compile_options(libs-apache-avro PRIVATE
  -DAVRO_SOURCE
  -DAVRO_VERSION="1.11.1"
  -DBOOST_ALL_NO_LIB
  -DBOOST_ATOMIC_DYN_LINK
  -DBOOST_FILESYSTEM_DYN_LINK
  -DBOOST_IOSTREAMS_DYN_LINK
  -DBOOST_PROGRAM_OPTIONS_DYN_LINK
  -DBOOST_REGEX_DYN_LINK
  -DBOOST_SYSTEM_DYN_LINK
  -DSNAPPY_CODEC_AVAILABLE
  -Wno-everything
)
target_include_directories(libs-apache-avro PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/api
)
target_link_libraries(libs-apache-avro PUBLIC
  contrib-libs-cxxsupp
  contrib-libs-snappy
  contrib-restricted-boost
  restricted-boost-asio
  restricted-boost-crc
  restricted-boost-iostreams
)
target_sources(libs-apache-avro PRIVATE
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/BinaryDecoder.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/BinaryEncoder.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Compiler.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/CustomFields.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/DataFile.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/FileStream.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Generic.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/GenericDatum.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/LogicalType.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Node.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/NodeImpl.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Resolver.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/ResolverSchema.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Schema.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Stream.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Types.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/ValidSchema.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Validator.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/Zigzag.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/json/JsonDom.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/json/JsonIO.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/parsing/JsonCodec.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/parsing/ResolvingDecoder.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/parsing/Symbol.cc
  ${CMAKE_SOURCE_DIR}/contrib/libs/apache/avro/impl/parsing/ValidatingCodec.cc
)
