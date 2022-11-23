
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(tx-tiering-rule)
target_link_libraries(tx-tiering-rule PUBLIC
  contrib-libs-cxxsupp
  yutil
  services-metadata-initializer
  services-metadata-abstract
)
target_sources(tx-tiering-rule PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tiering/rule/object.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tiering/rule/initializer.cpp
)

add_global_library_for(tx-tiering-rule.global tx-tiering-rule)
target_link_libraries(tx-tiering-rule.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  services-metadata-initializer
  services-metadata-abstract
)
target_sources(tx-tiering-rule.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tiering/rule/manager.cpp
)
