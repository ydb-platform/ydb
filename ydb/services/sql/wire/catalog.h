#pragma once

#include <util/generic/string.h>

namespace NKikimr::NSql {

// Fixed answers for the startup catalog reads. Returns false when the query is not in that list.
bool TryAnswerCatalog(TStringBuf sql, TString& reply);

// Build identity: release version, tag, or branch. Clients do not parse this text.
TString VersionText();

} // namespace NKikimr::NSql
