add_library(opt-physical-effects)
target_compile_options(opt-physical-effects PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(opt-physical-effects PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-kqp-common
  yql-dq-common
  yql-dq-opt
)
target_sources(opt-physical-effects PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/effects/kqp_opt_phy_delete_index.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/effects/kqp_opt_phy_effects.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/effects/kqp_opt_phy_indexes.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/effects/kqp_opt_phy_insert_index.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/effects/kqp_opt_phy_insert.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/effects/kqp_opt_phy_update_index.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/effects/kqp_opt_phy_update.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/effects/kqp_opt_phy_upsert_index.cpp
)
