
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(logical)
add_subdirectory(peephole)
add_subdirectory(physical)

add_library(core-kqp-opt)
target_compile_options(core-kqp-opt PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-opt PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-kqp-common
  kqp-opt-logical
  kqp-opt-peephole
  kqp-opt-physical
  yql-dq-common
  yql-dq-opt
)
target_sources(core-kqp-opt PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_build_txs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_effects.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_kql.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_phase.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_phy_check.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_phy_finalize.cpp
)
