#include "column_families.h"
#include "table_description.h"
#include "table_settings.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/follower_group.pb.h>

#include <ydb/library/conclusion/status.h>

#include <util/generic/list.h>
#include <util/string/builder.h>

namespace NKikimr {

void MEWarning(const TString& settingName, TList<TString>& warnings) {
    warnings.push_back(TStringBuilder() << "Table profile and " << settingName << " are set. They are mutually exclusive. "
        << "Use either one of them.");
}

namespace {
    const ui32 defaultMinPartitions = 1;
    const ui64 defaultSizeToSplit = 2ul << 30; // 2048 Mb

    template <typename TYdbProto>
    ui32 CalculateDefaultMinPartitions(const TYdbProto& proto) {
        switch (proto.partitions_case()) {
        case TYdbProto::kUniformPartitions:
            return static_cast<ui32>(proto.uniform_partitions());
        case TYdbProto::kPartitionAtKeys:
            return proto.partition_at_keys().split_points().size() + 1;
        default:
            return defaultMinPartitions;
        }
    }
}

template <class TYdbProto>
bool FillPartitioningPolicy(
    NKikimrSchemeOp::TPartitionConfig& partitionConfig,
    const TYdbProto& proto,
    Ydb::StatusIds::StatusCode& code, TString& error
) {
    const auto& partitioningSettings = proto.partitioning_settings();

    switch (partitioningSettings.partitioning_by_size()) {
    case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
    {
        if (partitioningSettings.partition_size_mb()) {
            auto &policy = *partitionConfig.MutablePartitioningPolicy();
            policy.SetSizeToSplit((1 << 20) * partitioningSettings.partition_size_mb());
            if (!partitioningSettings.min_partitions_count()) {
                policy.SetMinPartitionsCount(CalculateDefaultMinPartitions(proto));
            }
        }
        break;
    }
    case Ydb::FeatureFlag::ENABLED:
    {
        auto &policy = *partitionConfig.MutablePartitioningPolicy();
        if (partitioningSettings.partition_size_mb()) {
            policy.SetSizeToSplit((1 << 20) * partitioningSettings.partition_size_mb());
        } else {
            policy.SetSizeToSplit(defaultSizeToSplit);
        }
        if (!partitioningSettings.min_partitions_count()) {
            policy.SetMinPartitionsCount(CalculateDefaultMinPartitions(proto));
        }
        break;
    }
    case Ydb::FeatureFlag::DISABLED:
    {
        if (partitioningSettings.partition_size_mb()) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Auto partitioning partition size is set while "
                "auto partitioning by size is disabled";
            return false;
        }
        auto &policy = *partitionConfig.MutablePartitioningPolicy();
        policy.SetSizeToSplit(0);
        break;
    }
    default:
        code = Ydb::StatusIds::BAD_REQUEST;
        error = TStringBuilder() << "Unknown auto partitioning by size feature flag status: '"
            << (ui32)partitioningSettings.partitioning_by_size() << "'";
        return false;
    }

    switch (partitioningSettings.partitioning_by_load()) {
    case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
    {
        break;
    }
    case Ydb::FeatureFlag::ENABLED:
    {
        auto &policy = *partitionConfig.MutablePartitioningPolicy();
        policy.MutableSplitByLoadSettings()->SetEnabled(true);
        if (!partitioningSettings.min_partitions_count()) {
            policy.SetMinPartitionsCount(CalculateDefaultMinPartitions(proto));
        }
        break;
    }
    case Ydb::FeatureFlag::DISABLED:
    {
        auto &policy = *partitionConfig.MutablePartitioningPolicy();
        policy.MutableSplitByLoadSettings()->SetEnabled(false);
        break;
    }
    default:
        code = Ydb::StatusIds::BAD_REQUEST;
        error = TStringBuilder() << "Unknown auto partitioning by load feature flag status: '"
            << (ui32)partitioningSettings.partitioning_by_load() << "'";
        return false;
    }

    if (partitioningSettings.min_partitions_count()) {
        auto &policy = *partitionConfig.MutablePartitioningPolicy();
        policy.SetMinPartitionsCount(partitioningSettings.min_partitions_count());
    }

    if (partitioningSettings.max_partitions_count()) {
        auto &policy = *partitionConfig.MutablePartitioningPolicy();
        policy.SetMaxPartitionsCount(partitioningSettings.max_partitions_count());
    }

    return true;
}

template <typename TYdbProto>
bool FillPartitions(
    NKikimrSchemeOp::TTableDescription& tableDesc,
    const TYdbProto& proto,
    Ydb::StatusIds::StatusCode& code, TString& error
) {
    switch (proto.partitions_case()) {
    case TYdbProto::kUniformPartitions:
        tableDesc.SetUniformPartitionsCount(proto.uniform_partitions());
        break;
    case TYdbProto::kPartitionAtKeys:
        if (!CopyExplicitPartitions(tableDesc, proto.partition_at_keys(), code, error)) {
            return false;
        }
        break;
    default:
        break;
    }

    return true;
}

bool FillCreateTableSettingsDesc(NKikimrSchemeOp::TTableDescription& tableDesc,
    const Ydb::Table::CreateTableRequest& proto,
    Ydb::StatusIds::StatusCode& code, TString& error, TList<TString>& warnings, bool tableProfileSet)
{
    auto &partitionConfig = *tableDesc.MutablePartitionConfig();

    if (proto.has_partitioning_settings()) {
        if (tableProfileSet) {
            MEWarning("PartitioningSettings", warnings);
        }
        if (!FillPartitioningPolicy(partitionConfig, proto, code, error)) {
            return false;
        }
    }

    if (proto.partitions_case() != Ydb::Table::CreateTableRequest::PARTITIONS_NOT_SET) {
        if (tableProfileSet) {
            MEWarning("Partitions", warnings);
        }
        if (!FillPartitions(tableDesc, proto, code, error)) {
            return false;
        }
    }

    if (proto.key_bloom_filter() != Ydb::FeatureFlag::STATUS_UNSPECIFIED && tableProfileSet) {
        MEWarning("KeyBloomFilter", warnings);
    }
    switch (proto.key_bloom_filter()) {
    case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
        break;
    case Ydb::FeatureFlag::ENABLED:
    {
        partitionConfig.SetEnableFilterByKey(true);
        break;
    }
    case Ydb::FeatureFlag::DISABLED:
        partitionConfig.SetEnableFilterByKey(false);
        break;
    default:
        code = Ydb::StatusIds::BAD_REQUEST;
        error = TStringBuilder() << "Unknown key bloom filter feature flag status: '"
            << (ui32)proto.key_bloom_filter() << "'";
        return false;
    }

    if (proto.has_read_replicas_settings()) {
        if (tableProfileSet) {
            MEWarning("ReadReplicasSettings", warnings);
            partitionConfig.ClearFollowerCount();
            partitionConfig.ClearCrossDataCenterFollowerCount();
            partitionConfig.ClearAllowFollowerPromotion();
            partitionConfig.ClearFollowerGroups();
        }
        auto& readReplicasSettings = proto.read_replicas_settings();
        switch (readReplicasSettings.settings_case()) {
        case Ydb::Table::ReadReplicasSettings::kPerAzReadReplicasCount:
        {
            auto& followerGroup = *partitionConfig.AddFollowerGroups();
            followerGroup.SetFollowerCount(readReplicasSettings.per_az_read_replicas_count());
            followerGroup.SetRequireAllDataCenters(true);
            followerGroup.SetFollowerCountPerDataCenter(true);
            break;
        }
        case Ydb::Table::ReadReplicasSettings::kAnyAzReadReplicasCount:
        {
            auto& followerGroup = *partitionConfig.AddFollowerGroups();
            followerGroup.SetFollowerCount(readReplicasSettings.any_az_read_replicas_count());
            followerGroup.SetRequireAllDataCenters(false);
            break;
        }
        default:
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown read_replicas_settings type";
            return false;
        }
    }

    if (proto.has_ttl_settings()) {
        if (!FillTtlSettings(*tableDesc.MutableTTLSettings()->MutableEnabled(), proto.ttl_settings(), code, error)) {
            return false;
        }
    }

    if (proto.has_storage_settings()) {
        TColumnFamilyManager families(tableDesc.MutablePartitionConfig());
        if (!families.ApplyStorageSettings(proto.storage_settings(), &code, &error)) {
            return false;
        }
    }

    tableDesc.SetTemporary(proto.Gettemporary());

    return true;
}

bool FillCreateTableSettingsDesc(NKikimrSchemeOp::TColumnTableDescription& tableDesc,
    const Ydb::Table::CreateTableRequest& proto,
    Ydb::StatusIds::StatusCode& code, TString& error)
{
    auto& hashSharding = *tableDesc.MutableSharding()->MutableHashSharding();
    
    // NOTICE: public Ydb::Table::CreateTableRequest doesn't have sharding setting
    hashSharding.SetFunction(NKikimrSchemeOp::TColumnTableSharding::THashSharding::HASH_FUNCTION_CONSISTENCY_64);

    if (proto.has_partitioning_settings()) {
        auto& partitioningSettings = proto.partitioning_settings();
        hashSharding.MutableColumns()->CopyFrom(partitioningSettings.partition_by());
        if (partitioningSettings.min_partitions_count()) {
            tableDesc.SetColumnShardCount(partitioningSettings.min_partitions_count());
        }
    }
    
    if (proto.partitions_case() != Ydb::Table::CreateTableRequest::PARTITIONS_NOT_SET) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "Partitions are not supported";
        return false;
    }

    if (proto.key_bloom_filter() != Ydb::FeatureFlag::STATUS_UNSPECIFIED) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "Key bloom filter settings are not supported";
        return false;
    }
    
    if (proto.has_read_replicas_settings()) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "Read replicas settings are not supported";
        return false;
    }

    if (proto.has_ttl_settings()) {
        if (!FillTtlSettings(*tableDesc.MutableTtlSettings()->MutableEnabled(), proto.ttl_settings(), code, error)) {
            return false;
        }
    }
    
    if (proto.has_storage_settings()) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = "Storage settings are not supported";
        return false;
    }
    
    tableDesc.SetTemporary(proto.temporary());

    return true;
}

bool FillAlterTableSettingsDesc(NKikimrSchemeOp::TTableDescription& tableDesc,
    const Ydb::Table::AlterTableRequest& proto,
    Ydb::StatusIds::StatusCode& code, TString& error, bool changed)
{
    bool hadPartitionConfig = tableDesc.HasPartitionConfig();
    auto &partitionConfig = *tableDesc.MutablePartitionConfig();

    if (proto.has_alter_partitioning_settings()) {
        auto& alterSettings = proto.alter_partitioning_settings();

        switch (alterSettings.partitioning_by_size()) {
        case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
            if (alterSettings.partition_size_mb()) {
                auto &policy = *partitionConfig.MutablePartitioningPolicy();
                policy.SetSizeToSplit((1 << 20) * alterSettings.partition_size_mb());
                changed = true;
            }
            break;
        case Ydb::FeatureFlag::ENABLED:
        {
            auto &policy = *partitionConfig.MutablePartitioningPolicy();
            if (alterSettings.partition_size_mb()) {
                policy.SetSizeToSplit((1 << 20) * alterSettings.partition_size_mb());
            } else {
                policy.SetSizeToSplit(defaultSizeToSplit);
            }
            if (!alterSettings.min_partitions_count()) {
                policy.SetMinPartitionsCount(defaultMinPartitions);
            }
            changed = true;
            break;
        }
        case Ydb::FeatureFlag::DISABLED:
        {
            if (alterSettings.partition_size_mb()) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Auto partitioning partition size is set while "
                    "auto partitioning by size is disabled";
                return false;
            }
            auto &policy = *partitionConfig.MutablePartitioningPolicy();
            policy.SetSizeToSplit(0);
            changed = true;
            break;
        }
        default:
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown auto partitioning by size feature flag status: '"
                << (ui32)alterSettings.partitioning_by_size() << "'";
            return false;
        }

        switch (alterSettings.partitioning_by_load()) {
        case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
        {
            changed = true;
            break;
        }
        case Ydb::FeatureFlag::ENABLED:
        {
            auto &policy = *partitionConfig.MutablePartitioningPolicy();
            policy.MutableSplitByLoadSettings()->SetEnabled(true);
            if (!alterSettings.min_partitions_count()) {
                policy.SetMinPartitionsCount(defaultMinPartitions);
            }
            changed = true;
            break;
        }
        case Ydb::FeatureFlag::DISABLED:
        {
            auto &policy = *partitionConfig.MutablePartitioningPolicy();
            policy.MutableSplitByLoadSettings()->SetEnabled(false);
            changed = true;
            break;
        }
        default:
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown auto partitioning by load feature flag status: '"
                << (ui32)alterSettings.partitioning_by_load() << "'";
            return false;
        }

        if (alterSettings.min_partitions_count()) {
            auto &policy = *partitionConfig.MutablePartitioningPolicy();
            policy.SetMinPartitionsCount(alterSettings.min_partitions_count());
            changed = true;
        }

        if (alterSettings.max_partitions_count()) {
            auto &policy = *partitionConfig.MutablePartitioningPolicy();
            policy.SetMaxPartitionsCount(alterSettings.max_partitions_count());
            changed = true;
        }
    }

    switch (proto.set_key_bloom_filter()) {
    case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
        break;
    case Ydb::FeatureFlag::ENABLED:
        partitionConfig.SetEnableFilterByKey(true);
        changed = true;
        break;
    case Ydb::FeatureFlag::DISABLED:
        partitionConfig.SetEnableFilterByKey(false);
        changed = true;
        break;
    default:
        code = Ydb::StatusIds::BAD_REQUEST;
        error = TStringBuilder() << "Unknown key bloom filter feature flag status: '"
            << (ui32)proto.set_key_bloom_filter() << "'";
        return false;
    }

    if (proto.has_set_read_replicas_settings()) {
        auto& readReplicasSettings = proto.set_read_replicas_settings();
        switch (readReplicasSettings.settings_case()) {
        case Ydb::Table::ReadReplicasSettings::kPerAzReadReplicasCount:
        {
            auto& followerGroup = *partitionConfig.AddFollowerGroups();
            followerGroup.SetFollowerCount(readReplicasSettings.per_az_read_replicas_count());
            followerGroup.SetRequireAllDataCenters(true);
            followerGroup.SetFollowerCountPerDataCenter(true);
            break;
        }
        case Ydb::Table::ReadReplicasSettings::kAnyAzReadReplicasCount:
        {
            auto& followerGroup = *partitionConfig.AddFollowerGroups();
            followerGroup.SetFollowerCount(readReplicasSettings.any_az_read_replicas_count());
            followerGroup.SetRequireAllDataCenters(false);
            break;
        }
        default:
            code = Ydb::StatusIds::BAD_REQUEST;
            error = TStringBuilder() << "Unknown read_replicas_settings type";
            return false;
        }
        changed = true;
    }

    if (proto.has_set_ttl_settings()) {
        if (!FillTtlSettings(*tableDesc.MutableTTLSettings()->MutableEnabled(), proto.set_ttl_settings(), code, error)) {
            return false;
        }
    } else if (proto.has_drop_ttl_settings()) {
        tableDesc.MutableTTLSettings()->MutableDisabled();
    }

    if (!changed && !hadPartitionConfig) {
        tableDesc.ClearPartitionConfig();
    }

    return true;
}

bool FillIndexTablePartitioning(
    std::vector<NKikimrSchemeOp::TTableDescription>& indexImplTableDescriptions,
    const Ydb::Table::TableIndex& index,
    Ydb::StatusIds::StatusCode& code, TString& error
) {
    auto fillIndexPartitioning = [&](const Ydb::Table::GlobalIndexSettings& settings, NKikimrSchemeOp::TTableDescription& indexImplTableDescription) {
        auto& partitionConfig = *indexImplTableDescription.MutablePartitionConfig();

        if (settings.has_partitioning_settings()) {
            if (!FillPartitioningPolicy(partitionConfig, settings, code, error)) {
                return false;
            }
        }
        if (settings.partitions_case() != Ydb::Table::GlobalIndexSettings::PARTITIONS_NOT_SET) {
            if (!FillPartitions(indexImplTableDescription, settings, code, error)) {
                return false;
            }
        }
        if (settings.has_read_replicas_settings()) {
            const auto& readReplicasSettings = settings.read_replicas_settings();
            switch (readReplicasSettings.settings_case()) {
            case Ydb::Table::ReadReplicasSettings::kPerAzReadReplicasCount:
            {
                auto& followerGroup = *partitionConfig.AddFollowerGroups();
                followerGroup.SetFollowerCount(readReplicasSettings.per_az_read_replicas_count());
                followerGroup.SetRequireAllDataCenters(true);
                followerGroup.SetFollowerCountPerDataCenter(true);
                break;
            }
            case Ydb::Table::ReadReplicasSettings::kAnyAzReadReplicasCount:
            {
                auto& followerGroup = *partitionConfig.AddFollowerGroups();
                followerGroup.SetFollowerCount(readReplicasSettings.any_az_read_replicas_count());
                followerGroup.SetRequireAllDataCenters(false);
                break;
            }
            default:
                code = Ydb::StatusIds::BAD_REQUEST;
                error = TStringBuilder() << "Unknown read_replicas_settings type";
                return false;
            }
        }
        return true;
    };

    switch (index.type_case()) {
    case Ydb::Table::TableIndex::kGlobalIndex:
        indexImplTableDescriptions.resize(1);
        if (!fillIndexPartitioning(index.global_index().settings(), indexImplTableDescriptions[0])) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::kGlobalAsyncIndex:
        indexImplTableDescriptions.resize(1);
        if (!fillIndexPartitioning(index.global_async_index().settings(), indexImplTableDescriptions[0])) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::kGlobalUniqueIndex:
        indexImplTableDescriptions.resize(1);
        if (!fillIndexPartitioning(index.global_unique_index().settings(), indexImplTableDescriptions[0])) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::kGlobalVectorKmeansTreeIndex: {
        const bool prefixVectorIndex = index.index_columns().size() > 1;
        indexImplTableDescriptions.resize(prefixVectorIndex ? 3 : 2);
        if (!fillIndexPartitioning(index.global_vector_kmeans_tree_index().level_table_settings(), indexImplTableDescriptions[NTableIndex::NKMeans::LevelTablePosition])) {
            return false;
        }
        if (!fillIndexPartitioning(index.global_vector_kmeans_tree_index().posting_table_settings(), indexImplTableDescriptions[NTableIndex::NKMeans::PostingTablePosition])) {
            return false;
        }
        if (prefixVectorIndex) {
            if (!fillIndexPartitioning(index.global_vector_kmeans_tree_index().prefix_table_settings(), indexImplTableDescriptions[NTableIndex::NKMeans::PrefixTablePosition])) {
                return false;
            }
        }
        break;
    }

    case Ydb::Table::TableIndex::kGlobalFulltextPlainIndex:
        indexImplTableDescriptions.resize(1);
        if (!fillIndexPartitioning(index.global_fulltext_plain_index().settings(), indexImplTableDescriptions[0])) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::kGlobalFulltextRelevanceIndex:
        indexImplTableDescriptions.resize(4);
        if (!fillIndexPartitioning(index.global_fulltext_relevance_index().dict_table_settings(), indexImplTableDescriptions[NTableIndex::NFulltext::DictTablePosition])) {
            return false;
        }
        if (!fillIndexPartitioning(index.global_fulltext_relevance_index().docs_table_settings(), indexImplTableDescriptions[NTableIndex::NFulltext::DocsTablePosition])) {
            return false;
        }
        if (!fillIndexPartitioning(index.global_fulltext_relevance_index().stats_table_settings(), indexImplTableDescriptions[NTableIndex::NFulltext::StatsTablePosition])) {
            return false;
        }
        if (!fillIndexPartitioning(index.global_fulltext_relevance_index().posting_table_settings(), indexImplTableDescriptions[NTableIndex::NFulltext::PostingTablePosition])) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::kLocalBloomFilterIndex:
    case Ydb::Table::TableIndex::kLocalBloomNgramFilterIndex:
        break;

    case Ydb::Table::TableIndex::TYPE_NOT_SET:
        break;
    }

    return true;
}

namespace {

template <typename MutableDateTypeColumn, typename MutableValueSinceUnixEpoch>
TConclusionStatus FillTtlExpressionImpl(MutableDateTypeColumn&& mutable_date_type_column,
    MutableValueSinceUnixEpoch&& mutable_value_since_unix_epoch, const TString& column, const NKikimrSchemeOp::TTTLSettings::EUnit unit,
    const ui32 expireAfterSeconds) {
    switch (unit) {
    case NKikimrSchemeOp::TTTLSettings::UNIT_AUTO: {
        auto* mode = mutable_date_type_column();
        mode->set_column_name(column);
        mode->set_expire_after_seconds(expireAfterSeconds);
    } break;

    case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
    case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
    case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
    case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS: {
        auto* mode = mutable_value_since_unix_epoch();
        mode->set_column_name(column);
        mode->set_column_unit(static_cast<Ydb::Table::ValueSinceUnixEpochModeSettings::Unit>(unit));
        mode->set_expire_after_seconds(expireAfterSeconds);
    } break;

    default:
        return TConclusionStatus::Fail("Undefined column unit");
    }
    return TConclusionStatus::Success();
};

TConclusionStatus FillLegacyTtlMode(
    Ydb::Table::TtlSettings& out, const TString& column, const NKikimrSchemeOp::TTTLSettings::EUnit unit, const ui32 expireAfterSeconds) {
    return FillTtlExpressionImpl(
        [&out]() mutable {
            return out.mutable_date_type_column();
        },
        [&out]() mutable {
            return out.mutable_value_since_unix_epoch();
        },
        column, unit, expireAfterSeconds);
}

}   // namespace

template <typename TTtl>
bool FillPublicTtlSettingsImpl(Ydb::Table::TtlSettings& out, const TTtl& in, Ydb::StatusIds::StatusCode& code, TString& error) {
    auto bad_request = [&code, &error](const TString& message) -> bool {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = message;
        return false;
    };

    if (!in.TiersSize()) {
        // handle legacy input format for backwards-compatibility
        const auto status = FillLegacyTtlMode(out, in.GetColumnName(), in.GetColumnUnit(), in.GetExpireAfterSeconds());
        if (status.IsFail()) {
            return bad_request(status.GetErrorMessage());
        }
    } else if (in.TiersSize() == 1 && in.GetTiers(0).HasDelete()) {
        // convert delete-only TTL to legacy mode for backwards-compatibility
        const auto& tier = in.GetTiers(0);
        const auto status = FillLegacyTtlMode(out, in.GetColumnName(), in.GetColumnUnit(),
            tier.GetApplyAfterSeconds());
        if (status.IsFail()) {
            return bad_request(status.GetErrorMessage());
        }
    } else {
        for (const auto& inTier : in.GetTiers()) {
            auto& outTier = *out.mutable_tiered_ttl()->add_tiers();
            auto exprStatus = FillTtlExpressionImpl(
                [&outTier]() mutable {
                    return outTier.mutable_date_type_column();
                },
                [&outTier]() mutable {
                    return outTier.mutable_value_since_unix_epoch();
                },
                in.GetColumnName(), in.GetColumnUnit(), inTier.GetApplyAfterSeconds());
            if (exprStatus.IsFail()) {
                return bad_request(exprStatus.GetErrorMessage());
            }

            switch (inTier.GetActionCase()) {
            case NKikimrSchemeOp::TTTLSettings::TTier::ActionCase::kDelete:
                outTier.mutable_delete_();
                break;
            case NKikimrSchemeOp::TTTLSettings::TTier::ActionCase::kEvictToExternalStorage:
                outTier.mutable_evict_to_external_storage()->set_storage(inTier.GetEvictToExternalStorage().GetStorage());
                break;
            case NKikimrSchemeOp::TTTLSettings::TTier::ActionCase::ACTION_NOT_SET:
                return bad_request("Undefined tier action");
            }
        }
    }

    if constexpr (std::is_same_v<TTtl, NKikimrSchemeOp::TTTLSettings::TEnabled>) {
        if (in.HasSysSettings() && in.GetSysSettings().HasRunInterval()) {
            out.set_run_interval_seconds(TDuration::FromValue(in.GetSysSettings().GetRunInterval()).Seconds());
        }
    }

    return true;
}

template <typename TTtl>
bool FillSchemeTtlSettingsImpl(TTtl& out, const Ydb::Table::TtlSettings& in, Ydb::StatusIds::StatusCode& code, TString& error) {
    auto unsupported = [&code, &error](const TString& message) -> bool {
        code = Ydb::StatusIds::UNSUPPORTED;
        error = message;
        return false;
    };
    auto bad_request = [&code, &error](const TString& message) -> bool {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = message;
        return false;
    };

    auto setColumnUnit = [&unsupported](TTtl& out, const Ydb::Table::ValueSinceUnixEpochModeSettings::Unit unit) -> bool {
#define CASE_UNIT(type)                                         \
    case Ydb::Table::ValueSinceUnixEpochModeSettings::type:     \
        out.SetColumnUnit(NKikimrSchemeOp::TTTLSettings::type); \
        break

        switch (unit) {
            CASE_UNIT(UNIT_SECONDS);
            CASE_UNIT(UNIT_MILLISECONDS);
            CASE_UNIT(UNIT_MICROSECONDS);
            CASE_UNIT(UNIT_NANOSECONDS);
            default:
                return unsupported(TStringBuilder() << "Unsupported unit: " << static_cast<ui32>(unit));
        }
        return true;

#undef CASE_UNIT
    };

    switch (in.mode_case()) {
        case Ydb::Table::TtlSettings::kDateTypeColumn: {
            const auto& mode = in.date_type_column();
            auto* tier = out.AddTiers();
            tier->MutableDelete();
            tier->SetApplyAfterSeconds(mode.expire_after_seconds());
            out.SetColumnName(mode.column_name());
        } break;

        case Ydb::Table::TtlSettings::kValueSinceUnixEpoch: {
            const auto& mode = in.value_since_unix_epoch();
            auto* tier = out.AddTiers();
            tier->MutableDelete();
            tier->SetApplyAfterSeconds(mode.expire_after_seconds());
            out.SetColumnName(mode.column_name());
            if (!setColumnUnit(out, mode.column_unit())) {
                return false;
            }
        } break;

        case Ydb::Table::TtlSettings::kTieredTtl: {
            if (!in.tiered_ttl().tiers_size()) {
                return bad_request("No tiers in TTL settings");
            }

            std::optional<TString> columnName;
            std::optional<Ydb::Table::ValueSinceUnixEpochModeSettings::Unit> columnUnit;
            std::optional<Ydb::Table::TtlTier::ExpressionCase> expressionType;
            for (const auto& inTier : in.tiered_ttl().tiers()) {
                auto* outTier = out.AddTiers();
                TStringBuf tierColumnName;
                switch (inTier.expression_case()) {
                    case Ydb::Table::TtlTier::kDateTypeColumn: {
                        const auto& mode = inTier.date_type_column();
                        outTier->SetApplyAfterSeconds(mode.expire_after_seconds());
                        tierColumnName = mode.column_name();
                    } break;
                    case Ydb::Table::TtlTier::kValueSinceUnixEpoch: {
                        const auto& mode = inTier.value_since_unix_epoch();
                        outTier->SetApplyAfterSeconds(mode.expire_after_seconds());
                        tierColumnName = mode.column_name();
                        if (columnUnit) {
                            if (*columnUnit != mode.column_unit()) {
                                return bad_request(TStringBuilder()
                                                   << "Unit of the TTL columns must be the same for all tiers: "
                                                   << Ydb::Table::ValueSinceUnixEpochModeSettings::Unit_Name(*columnUnit)
                                                   << " != " << Ydb::Table::ValueSinceUnixEpochModeSettings::Unit_Name(mode.column_unit()));
                            }
                        } else {
                            columnUnit = mode.column_unit();
                        }
                    } break;
                    case Ydb::Table::TtlTier::EXPRESSION_NOT_SET:
                        return bad_request("Tier expression is undefined");
                }

                if (columnName) {
                    if (*columnName != tierColumnName) {
                        return bad_request(TStringBuilder() << "TTL columns must be the same for all tiers: " << *columnName << " != " << tierColumnName);
                    }
                } else {
                    columnName = tierColumnName;
                }

                if (expressionType) {
                    if (*expressionType != inTier.expression_case()) {
                        return bad_request("Expression type must be the same for all tiers");
                    }
                } else {
                    expressionType = inTier.expression_case();
                }

                switch (inTier.action_case()) {
                    case Ydb::Table::TtlTier::kDelete:
                        outTier->MutableDelete();
                        break;
                    case Ydb::Table::TtlTier::kEvictToExternalStorage:
                        outTier->MutableEvictToExternalStorage()->SetStorage(inTier.evict_to_external_storage().storage());
                        break;
                    case Ydb::Table::TtlTier::ACTION_NOT_SET:
                        return bad_request("Tier action is undefined");
                }
            }

            out.SetColumnName(*columnName);
            if (columnUnit) {
                setColumnUnit(out, *columnUnit);
            }
        } break;

        case Ydb::Table::TtlSettings::MODE_NOT_SET:
            return bad_request("TTL mode is undefined");
    }

    std::optional<ui32> expireAfterSeconds;
    for (const auto& tier : out.GetTiers()) {
        if (tier.HasDelete()) {
            expireAfterSeconds = tier.GetApplyAfterSeconds();
        }
    }
    out.SetExpireAfterSeconds(expireAfterSeconds.value_or(std::numeric_limits<uint32_t>::max()));

    return true;
}

bool FillTtlSettings(Ydb::Table::TtlSettings& out, const NKikimrSchemeOp::TTTLSettings::TEnabled& in, Ydb::StatusIds::StatusCode& code, TString& error) {
    return FillPublicTtlSettingsImpl(out, in, code, error);
}

bool FillTtlSettings(Ydb::Table::TtlSettings& out, const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& in, Ydb::StatusIds::StatusCode& code, TString& error) {
    return FillPublicTtlSettingsImpl(out, in, code, error);
}

bool FillTtlSettings(NKikimrSchemeOp::TTTLSettings::TEnabled& out, const Ydb::Table::TtlSettings& in, Ydb::StatusIds::StatusCode& code, TString& error) {
    if (!FillSchemeTtlSettingsImpl(out, in, code, error)) {
        return false;
    }

    if (in.run_interval_seconds()) {
        out.MutableSysSettings()->SetRunInterval(TDuration::Seconds(in.run_interval_seconds()).GetValue());
    }

    return true;
}

bool FillTtlSettings(NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& out, const Ydb::Table::TtlSettings& in, Ydb::StatusIds::StatusCode& code, TString& error) {
    return FillSchemeTtlSettingsImpl(out, in, code, error);
}

} // namespace NKikimr
