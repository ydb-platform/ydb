#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

namespace NKikimr {

    class TColumnFamilyManager {
    public:
        TColumnFamilyManager(NKikimrSchemeOp::TPartitionConfig* partitionConfig)
            : PartitionConfig(partitionConfig)
        { }

        const NKikimrSchemeOp::TFamilyDescription* FindDefaultFamily() const {
            if (DefaultFamilyIndex != size_t(-1)) {
                return &PartitionConfig->GetColumnFamilies(DefaultFamilyIndex);
            }

            for (size_t index = 0; index < PartitionConfig->ColumnFamiliesSize(); ++index) {
                const auto& family = PartitionConfig->GetColumnFamilies(index);
                if (IsDefaultFamily(family)) {
                    return &family;
                }
            }

            return nullptr;
        }

        NKikimrSchemeOp::TFamilyDescription* MutableDefaultFamily() {
            Modified = true;

            if (DefaultFamilyIndex != size_t(-1)) {
                return PartitionConfig->MutableColumnFamilies(DefaultFamilyIndex);
            }

            for (size_t index = 0; index < PartitionConfig->ColumnFamiliesSize(); ++index) {
                const auto& family = PartitionConfig->GetColumnFamilies(index);
                if (IsDefaultFamily(family)) {
                    DefaultFamilyIndex = index;
                    return PartitionConfig->MutableColumnFamilies(index);
                }
            }

            DefaultFamilyIndex = PartitionConfig->ColumnFamiliesSize();
            auto* family = PartitionConfig->AddColumnFamilies();
            family->SetId(0);
            return family;
        }

        NKikimrSchemeOp::TFamilyDescription* MutableNamedFamily(const TString& name) {
            Modified = true;

            if (name == "default") {
                return MutableDefaultFamily();
            }

            if (NamedFamilyIndex.empty() && PartitionConfig->ColumnFamiliesSize() > 0) {
                for (size_t index = 0; index < PartitionConfig->ColumnFamiliesSize(); ++index) {
                    const auto& family = PartitionConfig->GetColumnFamilies(index);
                    if (family.HasName()) {
                        NamedFamilyIndex.emplace(family.GetName(), index);
                    }
                }
            }

            auto it = NamedFamilyIndex.find(name);
            if (it != NamedFamilyIndex.end()) {
                return PartitionConfig->MutableColumnFamilies(it->second);
            }

            NamedFamilyIndex.emplace(name, PartitionConfig->ColumnFamiliesSize());
            auto* family = PartitionConfig->AddColumnFamilies();
            family->SetName(name);
            return family;
        }

        bool ApplyStorageSettings(
                const Ydb::Table::StorageSettings& settings,
                Ydb::StatusIds::StatusCode* code,
                TString* error)
        {
            if (settings.has_tablet_commit_log0()) {
                auto* dst = MutableDefaultFamily()->MutableStorageConfig()->MutableSysLog();
                dst->SetPreferredPoolKind(settings.tablet_commit_log0().media());
                dst->SetAllowOtherKinds(false);
            }

            if (settings.has_tablet_commit_log1()) {
                auto* dst = MutableDefaultFamily()->MutableStorageConfig()->MutableLog();
                dst->SetPreferredPoolKind(settings.tablet_commit_log1().media());
                dst->SetAllowOtherKinds(false);
            }

            if (settings.has_external()) {
                auto* dst = MutableDefaultFamily()->MutableStorageConfig()->MutableExternal();
                dst->SetPreferredPoolKind(settings.external().media());
                dst->SetAllowOtherKinds(false);
            }

            switch (settings.store_external_blobs()) {
                case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
                    break;
                case Ydb::FeatureFlag::ENABLED: {
                    if (!AppData()->FeatureFlags.GetEnablePublicApiExternalBlobs()) {
                        *code = Ydb::StatusIds::BAD_REQUEST;
                        *error = TStringBuilder()
                            << "Setting store_external_blobs to ENABLED is not allowed";
                        return false;
                    }
                    auto* dst = MutableDefaultFamily()->MutableStorageConfig();
                    // TODO: it probably needs to be configurable?
                    dst->SetExternalThreshold(512 * 1024);
                    break;
                }
                case Ydb::FeatureFlag::DISABLED: {
                    auto* dst = MutableDefaultFamily()->MutableStorageConfig();
                    if (dst->HasExternalThreshold()) {
                        dst->ClearExternalThreshold();
                    }
                    break;
                }
                default: {
                    *code = Ydb::StatusIds::BAD_REQUEST;
                    *error = TStringBuilder()
                        << "unknown store_external_blobs feature flag status "
                        << (ui32)settings.store_external_blobs();
                    return false;
                }
            }

            return true;
        }

        bool ApplyFamilySettings(
                const Ydb::Table::ColumnFamily& familySettings,
                Ydb::StatusIds::StatusCode* code,
                TString* error)
        {
            if (familySettings.name().empty()) {
                *code = Ydb::StatusIds::BAD_REQUEST;
                *error = "Missing column family name";
                return false;
            }

            auto* family = MutableNamedFamily(familySettings.name());

            if (familySettings.has_data()) {
                auto* dst = family->MutableStorageConfig()->MutableData();
                dst->SetPreferredPoolKind(familySettings.data().media());
                dst->SetAllowOtherKinds(false);
            }

            switch (familySettings.compression()) {
                case Ydb::Table::ColumnFamily::COMPRESSION_UNSPECIFIED:
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_NONE:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecPlain);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_GZIP:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecGZIP);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_SNAPPY:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecSNAPPY);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_LZO:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZO);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_BROTLI:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecBROTLI);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_LZ4_RAW:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4RAW);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_LZ4:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_LZ4_HADOOP:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4HADOOP);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_ZSTD:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecZSTD);
                    break;
                case Ydb::Table::ColumnFamily::COMPRESSION_BZ2:
                    family->SetColumnCodec(NKikimrSchemeOp::ColumnCodecBZ2);
                    break;
                default:
                    *code = Ydb::StatusIds::BAD_REQUEST;
                    *error = TStringBuilder() << "Unsupported compression value " << (ui32)familySettings.compression() << " in column family '"
                                              << familySettings.name() << "'";
                    return false;
            }

            switch (familySettings.keep_in_memory()) {
                case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
                    break;
                case Ydb::FeatureFlag::ENABLED:
                    *code = Ydb::StatusIds::BAD_REQUEST;
                    *error = TStringBuilder()
                        << "Setting keep_in_memory to ENABLED is not supported in column family '"
                        << familySettings.name() << "'";
                    return false;
                case Ydb::FeatureFlag::DISABLED:
                    family->ClearColumnCache();
                    break;
                default:
                    *code = Ydb::StatusIds::BAD_REQUEST;
                    *error = TStringBuilder()
                        << "Unsupported keep_in_memory value "
                        << (ui32)familySettings.keep_in_memory()
                        << " in column family '" << familySettings.name() << "'";
                    return false;
            }

            return true;
        }

        bool ValidateColumnFamilies(
                Ydb::StatusIds::StatusCode* code,
                TString* error) const
        {
            if (PartitionConfig->ColumnFamiliesSize() == 0) {
                // No column families defined, perfectly ok
                return true;
            }

            const auto* defaultFamily = FindDefaultFamily();
            if (!defaultFamily) {
                *code = Ydb::StatusIds::BAD_REQUEST;
                *error = TStringBuilder()
                    << "Missing 'default' column family in the table definition";
                return false;
            }

            if (!defaultFamily->HasStorageConfig() ||
                !defaultFamily->GetStorageConfig().HasSysLog() ||
                !defaultFamily->GetStorageConfig().HasLog())
            {
                *code = Ydb::StatusIds::BAD_REQUEST;
                *error = TStringBuilder()
                    << "Column families cannot be used without tablet_commit_log0 and tablet_commit_log1 media defined";
                return false;
            }

            return true;
        }

    public:
        static bool IsDefaultFamily(const NKikimrSchemeOp::TFamilyDescription& family) {
            if (family.HasId() && family.GetId() == 0) {
                return true; // explicit id 0
            }
            if (!family.HasId() && !family.HasName()) {
                return true; // neither id nor name specified
            }
            return false;
        }

    public:
        NKikimrSchemeOp::TPartitionConfig* const PartitionConfig;
        bool Modified = false;

    private:
        THashMap<TString, size_t> NamedFamilyIndex;
        size_t DefaultFamilyIndex = -1;
    };

} // namespace NKikimr
