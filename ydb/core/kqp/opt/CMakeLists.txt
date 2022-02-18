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
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_join.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_kql.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_phase.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_phy_check.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/kqp_opt_phy_finalize.cpp
)
