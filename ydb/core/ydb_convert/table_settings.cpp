#include "table_description.h"
#include "table_settings.h"
#include "column_families.h"

#include <ydb/core/protos/follower_group.pb.h>

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

    if (proto.tiering().size()) {
        tableDesc.MutableTTLSettings()->SetUseTiering(proto.tiering());
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

    if (proto.has_set_tiering()) {
        tableDesc.MutableTTLSettings()->SetUseTiering(proto.set_tiering());
    } else if (proto.has_drop_tiering()) {
        tableDesc.MutableTTLSettings()->SetUseTiering("");
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
    auto fillIndexPartitioning = [&](const Ydb::Table::GlobalIndexSettings& settings, std::vector<NKikimrSchemeOp::TTableDescription>& indexImplTableDescriptions) {
        indexImplTableDescriptions.push_back({});
        auto& indexImplTableDescription = indexImplTableDescriptions.back();

        if (settings.has_partitioning_settings()) {
            if (!FillPartitioningPolicy(*indexImplTableDescription.MutablePartitionConfig(), settings, code, error)) {
                return false;
            }
        }
        if (settings.partitions_case() != Ydb::Table::GlobalIndexSettings::PARTITIONS_NOT_SET) {
            if (!FillPartitions(indexImplTableDescription, settings, code, error)) {
                return false;
            }
        }
        return true;
    };

    switch (index.type_case()) {
    case Ydb::Table::TableIndex::kGlobalIndex:
        if (!fillIndexPartitioning(index.global_index().settings(), indexImplTableDescriptions)) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::kGlobalAsyncIndex:
        if (!fillIndexPartitioning(index.global_async_index().settings(), indexImplTableDescriptions)) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::kGlobalUniqueIndex:
        if (!fillIndexPartitioning(index.global_unique_index().settings(), indexImplTableDescriptions)) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::kGlobalVectorKmeansTreeIndex:
        if (!fillIndexPartitioning(index.global_vector_kmeans_tree_index().level_table_settings(), indexImplTableDescriptions)) {
            return false;
        }
        if (!fillIndexPartitioning(index.global_vector_kmeans_tree_index().posting_table_settings(), indexImplTableDescriptions)) {
            return false;
        }
        break;

    case Ydb::Table::TableIndex::TYPE_NOT_SET:
        break;
    }

    return true;
}

} // namespace NKikimr
