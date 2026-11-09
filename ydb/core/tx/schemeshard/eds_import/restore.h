#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/string.h>
#include <variant>

namespace NKikimr::NSchemeShard {

using TImportedSchemeResult = std::variant<TString, NKikimrSchemeOp::TModifyScheme>;

// Run only on a newly prepared result, before it is persisted. Retried prepared payloads are already mapped.
void RebindImportedEdsInitialToken(Ydb::StatusIds::StatusCode& status, TImportedSchemeResult& result,
    TStringBuf creationQuery, TStringBuf destinationDatabase);

} // namespace NKikimr::NSchemeShard
