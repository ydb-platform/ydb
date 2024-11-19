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
void FillTtlSettings(Ydb::Table::TtlSettings& out, const NKikimrSchemeOp::TTTLSettings::TEnabled& in);
void FillTtlSettings(Ydb::Table::TtlSettings& out, const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& in);
// in
template <class TTtlSettingsEnabled>
bool FillTtlSettings(TTtlSettingsEnabled& out, const Ydb::Table::TtlSettings& in,
    Ydb::StatusIds::StatusCode& code, TString& error)
{
    auto unsupported = [&code, &error](const TString& message) -> bool {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = message;
        return false;
    };

    static const auto& fillModeSettings = []<class TModeSettings>(TTtlSettingsEnabled& out, const TModeSettings& in) {
        out.SetColumnName(in.column_name());
    };

    static const auto& fillDeleteTier = []<class TModeSettings>(TTtlSettingsEnabled& out, const TModeSettings& in) {
        auto* deleteTier = out.AddTiers();
        deleteTier->SetEvictAfterSeconds(in.expire_after_seconds());
        deleteTier->MutableDelete();
    };

    static const auto& fillColumnUnit = [&unsupported]<class TModeSettings> (TTtlSettingsEnabled& out, const TModeSettings& in) -> bool {
        #define CASE_UNIT(type) \
            case Ydb::Table::ValueSinceUnixEpochModeSettings::type: \
                out.SetColumnUnit(NKikimrSchemeOp::TTTLSettings::type); \
                break

        switch (in.column_unit()) {
        CASE_UNIT(UNIT_SECONDS);
        CASE_UNIT(UNIT_MILLISECONDS);
        CASE_UNIT(UNIT_MICROSECONDS);
        CASE_UNIT(UNIT_NANOSECONDS);
        default:
            return unsupported(TStringBuilder() << "Unsupported unit: "
                << static_cast<ui32>(in.column_unit()));
        }
        return true;

        #undef CASE_UNIT
    };

    if (in.tiers_size()) {
        for (const auto& inTier : in.tiers()) {
            auto* outTier = out.AddTiers();
            outTier->SetEvictAfterSeconds(inTier.evict_after_seconds());
            switch (inTier.action_case()) {
                case Ydb::Table::TtlTier::kDelete:
                    outTier->MutableDelete();
                    break;
                case Ydb::Table::TtlTier::kEvictToExternalStorage:
                    outTier->MutableEvictToExternalStorage()->SetStorageName(inTier.evict_to_external_storage().storage_name());
                    break;
                case Ydb::Table::TtlTier::ACTION_NOT_SET:
                    break;
            }
        }
    }

    switch (in.mode_case()) {
    case Ydb::Table::TtlSettings::kDeprecatedDateTypeColumn:
        fillModeSettings(out, in.deprecated_date_type_column());
        fillDeleteTier(out, in.deprecated_date_type_column());
        break;

    case Ydb::Table::TtlSettings::kDeprecatedValueSinceUnixEpoch:
        fillModeSettings(out, in.deprecated_value_since_unix_epoch());
        fillDeleteTier(out, in.deprecated_date_type_column());
        if (!fillColumnUnit(out, in.value_since_unix_epoch())) {
            return false;
        }
        break;

    case Ydb::Table::TtlSettings::kDateTypeColumn:
        fillModeSettings(out, in.date_type_column());
        break;

    case Ydb::Table::TtlSettings::kValueSinceUnixEpoch:
        fillModeSettings(out, in.value_since_unix_epoch());
        if (!fillColumnUnit(out, in.value_since_unix_epoch())) {
            return false;
        }
        break;

    case Ydb::Table::TtlSettings::MODE_NOT_SET:
        return unsupported("Unsupported ttl settings");
    }

    std::optional<ui32> expireInSeconds = 0;
    for (const auto& tier : out.GetTiers()) {
        if (tier.HasDelete()) {
            expireInSeconds = tier.GetEvictAfterSeconds();
        }
    }
    out.SetExpireAfterSeconds(expireInSeconds.value_or(std::numeric_limits<uint32_t>::max()));

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
