#pragma once

#include <ydb/core/ydb_convert/table_profiles.h>
#include <ydb/core/ydb_convert/table_settings.h>

namespace NKikimr {
namespace NGRpcService {

class IAuditCtx;

bool FillCreateTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::CreateTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings);

void ResolveTtlStoragePaths(Ydb::Table::TtlSettings& settings, const IAuditCtx& request);

} // namespace NGRpcService
} // namespace NKikimr
