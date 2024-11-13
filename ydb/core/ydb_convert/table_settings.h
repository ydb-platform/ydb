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

    static const auto& fillCommonFields = []<class TModeSettings>(TTtlSettingsEnabled& out, const TModeSettings& in, std::optional<ui32> expireAfterSeconds) {
        out.SetColumnName(in.column_name());
        out.SetExpireAfterSeconds(expireAfterSeconds.value_or(in.expire_after_seconds()));
    };

    std::optional<ui32> expireAfterSeconds;
    if (in.tiers_size()) {
        for (const auto& inTier : in.tiers()) {
            auto* outTier = out.AddTiers();
            outTier->SetEvictAfterSeconds(inTier.evict_after_seconds());
            switch (inTier.action_case()) {
                case Ydb::Table::TtlTier::kDelete:
                    outTier->MutableDelete();
                    expireAfterSeconds = inTier.evict_after_seconds();
                    break;
                case Ydb::Table::TtlTier::kEvictToExternalStorage:
                    outTier->MutableEvictToExternalStorage()->SetStorageName(inTier.evict_to_external_storage().storage_name());
                    break;
                case Ydb::Table::TtlTier::kEvictToColumnFamily:
                    outTier->MutableEvictToColumnFamily()->SetFamilyName(inTier.evict_to_column_family().family_name());
                    break;
                case Ydb::Table::TtlTier::ACTION_NOT_SET:
                    break;
            }
        }
        if (!expireAfterSeconds) {
            expireAfterSeconds = std::numeric_limits<uint32_t>::max();
        }
    }

    switch (in.mode_case()) {
    case Ydb::Table::TtlSettings::kDateTypeColumn:
        fillCommonFields(out, in.date_type_column(), expireAfterSeconds);
        break;

    case Ydb::Table::TtlSettings::kValueSinceUnixEpoch:
        fillCommonFields(out, in.value_since_unix_epoch(), expireAfterSeconds);

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
