add_library(yq-libs-audit)
target_link_libraries(yq-libs-audit PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yq-libs-audit PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/audit/yq_audit_service.cpp
)
