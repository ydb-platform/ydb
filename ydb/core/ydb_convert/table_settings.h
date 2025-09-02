#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

namespace NKikimr {

void MEWarning(const TString& settingName, TList<TString>& warnings);

bool FillCreateTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::CreateTableRequest& in,
    Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings, bool tableProfileSet);

bool FillAlterTableSettingsDesc(NKikimrSchemeOp::TTableDescription& out,
    const Ydb::Table::AlterTableRequest& in,
    Ydb::StatusIds::StatusCode& code, TString& error, bool changed);


// out
bool FillTtlSettings(Ydb::Table::TtlSettings& out, const NKikimrSchemeOp::TTTLSettings::TEnabled& in, Ydb::StatusIds::StatusCode& code, TString& error);
bool FillTtlSettings(Ydb::Table::TtlSettings& out, const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& in, Ydb::StatusIds::StatusCode& code, TString& error);
// in
bool FillTtlSettings(NKikimrSchemeOp::TTTLSettings::TEnabled& out, const Ydb::Table::TtlSettings& in, Ydb::StatusIds::StatusCode& code, TString& error);
bool FillTtlSettings(NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& out, const Ydb::Table::TtlSettings& in, Ydb::StatusIds::StatusCode& code, TString& error);

bool FillIndexTablePartitioning(
    std::vector<NKikimrSchemeOp::TTableDescription>& indexImplTableDescriptions,
    const Ydb::Table::TableIndex& index,
    Ydb::StatusIds::StatusCode& code, TString& error);

} // namespace NKikimr
