add_library(libs-audit-mock)
target_link_libraries(libs-audit-mock PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-audit-events
  libs-config-protos
)
target_sources(libs-audit-mock PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/audit/mock/yq_mock_audit_service.cpp
)
