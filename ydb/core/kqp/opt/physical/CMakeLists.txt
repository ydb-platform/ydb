add_library(kqp-opt-physical)
target_compile_options(kqp-opt-physical PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(kqp-opt-physical PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-kqp-common
  opt-physical-effects
  yql-dq-common
  yql-dq-opt
)
target_sources(kqp-opt-physical PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/kqp_opt_phy_build_stage.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/kqp_opt_phy_limit.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/kqp_opt_phy_olap_filter.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/kqp_opt_phy_sort.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/kqp_opt_phy_helpers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/kqp_opt_phy_stage_float_up.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/physical/kqp_opt_phy.cpp
)
