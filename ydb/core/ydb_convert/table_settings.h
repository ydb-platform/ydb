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

template <class TTtlSettingsEnabled>
bool FillTtlSettings(TTtlSettingsEnabled& out, const Ydb::Table::TtlSettings& in,
    Ydb::StatusIds::StatusCode& code, TString& error)
{
    auto unsupported = [&code, &error](const TString& message) -> bool {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = message;
        return false;
    };

    switch (in.mode_case()) {
    case Ydb::Table::TtlSettings::kDateTypeColumn:
        out.SetColumnName(in.date_type_column().column_name());
        out.SetExpireAfterSeconds(in.date_type_column().expire_after_seconds());
        break;

    case Ydb::Table::TtlSettings::kValueSinceUnixEpoch:
        out.SetColumnName(in.value_since_unix_epoch().column_name());
        out.SetExpireAfterSeconds(in.value_since_unix_epoch().expire_after_seconds());

        #define CASE_UNIT(type) \
            case Ydb::Table::ValueSinceUnixEpochModeSettings::type: \
                out.SetColumnUnit(NKikimrSchemeOp::TTTLSettings::type); \
                break

        switch (in.value_since_unix_epoch().column_unit()) {
        CASE_UNIT(UNIT_SECONDS);
        CASE_UNIT(UNIT_MILLISECONDS);
        CASE_UNIT(UNIT_MICROSECONDS);
        CASE_UNIT(UNIT_NANOSECONDS);
        default:
            return unsupported(TStringBuilder() << "Unsupported unit: "
                << static_cast<ui32>(in.value_since_unix_epoch().column_unit()));
        }

        #undef CASE_UNIT
        break;

    default:
        return unsupported("Unsupported ttl settings");
    }

    if constexpr (std::is_same_v<TTtlSettingsEnabled, NKikimrSchemeOp::TTTLSettings::TEnabled>) {
        if (in.run_interval_seconds()) {
            out.MutableSysSettings()->SetRunInterval(TDuration::Seconds(in.run_interval_seconds()).GetValue());
        }
    }

    return true;
}

bool FillIndexTablePartitioning(
    std::vector<NKikimrSchemeOp::TTableDescription>& indexImplTableDescriptions,
    const Ydb::Table::TableIndex& index,
    Ydb::StatusIds::StatusCode& code, TString& error);

} // namespace NKikimr
