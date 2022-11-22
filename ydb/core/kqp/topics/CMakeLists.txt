
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-kqp-topics)
target_compile_options(core-kqp-topics PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-topics PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
)
target_sources(core-kqp-topics PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/topics/kqp_topics.cpp
)
