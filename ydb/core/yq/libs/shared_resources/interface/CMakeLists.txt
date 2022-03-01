
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(libs-shared_resources-interface)
target_compile_options(libs-shared_resources-interface PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(libs-shared_resources-interface PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
)
target_sources(libs-shared_resources-interface PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/shared_resources/interface/shared_resources.cpp
)
