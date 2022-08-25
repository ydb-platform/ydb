#pragma once

#include <ydb/core/ydb_convert/table_profiles.h>
#include <ydb/core/ydb_convert/table_settings.h>

namespace NKikimr {
namespace NGRpcService {

bool FillCreateTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::CreateTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings);

bool FillAlterTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::AlterTableRequest& in, const TTableProfiles& profiles,
    Ydb::StatusIds::StatusCode& code, TString& error, const TAppData* appData);

} // namespace NGRpcService
} // namespace NKikimr
