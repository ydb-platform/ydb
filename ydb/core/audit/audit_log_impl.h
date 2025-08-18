#pragma once
#include <ydb/core/audit/audit_log.h>

namespace NKikimr::NAudit {

void EscapeNonUtf8LogParts(TVector<std::pair<TString, TString>>& parts);

} // namespace NKikimr::NAudit
