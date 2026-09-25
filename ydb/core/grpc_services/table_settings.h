#pragma once

#include <ydb/core/ydb_convert/table_profiles.h>
#include <ydb/core/ydb_convert/table_settings.h>

namespace NKikimr {
namespace NGRpcService {

class IRequestCtxBaseMtSafe;

bool FillCreateTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::CreateTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings);

void NormalizeTtlStoragePaths(Ydb::Table::TtlSettings& settings, const IRequestCtxBaseMtSafe& request);


} // namespace NGRpcService
} // namespace NKikimr
