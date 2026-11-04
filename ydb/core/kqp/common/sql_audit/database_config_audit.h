#pragma once

#include <ydb/core/kqp/common/simple/database_config_audit.h>

namespace NKikimr::NKqp {

// Parse SQL without translating it, so duplicate settings and other early
// translation errors still have an audit record. Malformed SQL has no reliable
// target/changes and remains covered by the ordinary query audit log.
TVector<TDatabaseConfigAuditSettings> CollectDatabaseConfigAuditSettingsFromSql(const TString& query) noexcept;

} // namespace NKikimr::NKqp
