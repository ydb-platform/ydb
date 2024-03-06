#include "table_profiles.h"
#include "table_description.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/follower_group.pb.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/domain.h>

#include <util/string/printf.h>

namespace NKikimr {

TTableProfiles::TTableProfiles()
{
}

TTableProfiles::TTableProfiles(const NKikimrConfig::TTableProfilesConfig &config)
{
    Load(config);
}

void TTableProfiles::Load(const NKikimrConfig::TTableProfilesConfig &config)
{
    CompactionPolicies.clear();
    ExecutionPolicies.clear();
    PartitioningPolicies.clear();
    StoragePolicies.clear();
    ReplicationPolicies.clear();
    CachingPolicies.clear();
    TableProfiles.clear();

    for (auto &profile : config.GetTableProfiles())
        TableProfiles[profile.GetName()] = profile;
    for (auto &policy : config.GetCompactionPolicies())
        CompactionPolicies[policy.GetName()] = policy;
    for (auto &policy : config.GetExecutionPolicies())
        ExecutionPolicies[policy.GetName()] = policy;
    for (auto &policy : config.GetPartitioningPolicies())
        PartitioningPolicies[policy.GetName()] = policy;
    for (auto &policy : config.GetStoragePolicies())
        StoragePolicies[policy.GetName()] = policy;
    for (auto &policy : config.GetReplicationPolicies())
        ReplicationPolicies[policy.GetName()] = policy;
    for (auto &policy : config.GetCachingPolicies())
        CachingPolicies[policy.GetName()] = policy;
}

NKikimrSchemeOp::TFamilyDescription *TTableProfiles::GetNamedFamilyDescription(NKikimrConfig::TStoragePolicy &policy, const TString& name) const {
    for (size_t i = 0; i < policy.ColumnFamiliesSize(); ++i) {
        const auto& family = policy.GetColumnFamilies(i);
        if (family.HasName() && name == family.GetName()) {
            return policy.MutableColumnFamilies(i);
        }
    }
    auto res = policy.AddColumnFamilies();
    res->SetName(name);
    return res;
}

NKikimrSchemeOp::TFamilyDescription *TTableProfiles::GetDefaultFamilyDescription(NKikimrConfig::TStoragePolicy &policy) const {
    for (size_t i = 0; i < policy.ColumnFamiliesSize(); ++i) {
        const auto& family = policy.GetColumnFamilies(i);
        if ((family.HasId() && family.GetId() == 0) ||
            (!family.HasId() && !family.HasName()))
        {
            return policy.MutableColumnFamilies(i);
        }
    }
    auto res = policy.AddColumnFamilies();
    res->SetId(0);
    return res;
}

NKikimrSchemeOp::TStorageConfig *TTableProfiles::GetDefaultStorageConfig(NKikimrConfig::TStoragePolicy &policy) const {
    return GetDefaultFamilyDescription(policy)->MutableStorageConfig();
}

bool TTableProfiles::HasPresetName(const TString &presetName) const {
    return TableProfiles.contains(presetName);
}

bool TTableProfiles::ApplyTableProfile(const Ydb::Table::TableProfile &profile,
                                       NKikimrSchemeOp::TTableDescription &tableDesc,
                                       Ydb::StatusIds::StatusCode &code,
                                       TString &error) const
{
    NKikimrConfig::TCompactionPolicy compactionPolicy;
    NKikimrConfig::TExecutionPolicy executionPolicy;
    NKikimrConfig::TPartitioningPolicy partitioningPolicy;
    NKikimrConfig::TStoragePolicy storagePolicy;
    NKikimrConfig::TReplicationPolicy replicationPolicy;
    NKikimrConfig::TCachingPolicy cachingPolicy;
    auto &partitionConfig = *tableDesc.MutablePartitionConfig();

    if (profile.preset_name() && !TableProfiles.contains(profile.preset_name())) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = Sprintf("unknown table profile preset '%s'", profile.preset_name().data());
        return false;
    }

    TString name = profile.preset_name() ? profile.preset_name() : "default";
    NKikimrConfig::TTableProfile tableProfile;
    if (TableProfiles.contains(name))
        tableProfile = TableProfiles.at(name);

    // Determine used compaction policy.
    if (profile.has_compaction_policy()) {
        auto &policy = profile.compaction_policy();
        if (!CompactionPolicies.contains(policy.preset_name())) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("unknown compaction policy preset '%s'", policy.preset_name().data());
            return false;
        }
        compactionPolicy = CompactionPolicies.at(policy.preset_name());
    } else if (CompactionPolicies.contains(tableProfile.GetCompactionPolicy())) {
        compactionPolicy = CompactionPolicies.at(tableProfile.GetCompactionPolicy());
    }

    // Determine used execution policy.
    if (profile.has_execution_policy()) {
        auto &policy = profile.execution_policy();
        if (!ExecutionPolicies.contains(policy.preset_name())) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("unknown execution policy preset '%s'", policy.preset_name().data());
            return false;
        }
        executionPolicy = ExecutionPolicies.at(policy.preset_name());
    } else if (ExecutionPolicies.contains(tableProfile.GetExecutionPolicy())) {
        executionPolicy = ExecutionPolicies.at(tableProfile.GetExecutionPolicy());
    }

    // Determine used partitioning policy.
    if (profile.has_partitioning_policy()) {
        auto &policy = profile.partitioning_policy();
        if (policy.preset_name()) {
            if (!PartitioningPolicies.contains(policy.preset_name())) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf("unknown partitioning policy preset '%s'", policy.preset_name().data());
                return false;
            }
            partitioningPolicy = PartitioningPolicies.at(policy.preset_name());
        } else if (PartitioningPolicies.contains(tableProfile.GetPartitioningPolicy())) {
            partitioningPolicy = PartitioningPolicies.at(tableProfile.GetPartitioningPolicy());
        }
        // Apply auto partitioning overwrites.
        switch (policy.auto_partitioning()) {
        case Ydb::Table::PartitioningPolicy::AUTO_PARTITIONING_POLICY_UNSPECIFIED:
            break;
        case Ydb::Table::PartitioningPolicy::DISABLED:
            partitioningPolicy.SetAutoSplit(false);
            partitioningPolicy.SetAutoMerge(false);
            break;
        case Ydb::Table::PartitioningPolicy::AUTO_SPLIT:
            partitioningPolicy.SetAutoSplit(true);
            partitioningPolicy.SetAutoMerge(false);
            break;
        case Ydb::Table::PartitioningPolicy::AUTO_SPLIT_MERGE:
            partitioningPolicy.SetAutoSplit(true);
            partitioningPolicy.SetAutoMerge(true);
            break;
        default:
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("unknown auto partitioning policy %" PRIu32,
                            (ui32)policy.auto_partitioning());
            return false;
        }
        // Apply uniform partitions overwrites.
        switch (policy.partitions_case()) {
        case Ydb::Table::PartitioningPolicy::kUniformPartitions:
            partitioningPolicy.SetUniformPartitionsCount(policy.uniform_partitions());
            break;
        case Ydb::Table::PartitioningPolicy::kExplicitPartitions:
            if (!CopyExplicitPartitions(tableDesc, policy.explicit_partitions(), code, error))
                return false;
            break;
        default:
            break;
        }
    } else if (PartitioningPolicies.contains(tableProfile.GetPartitioningPolicy())) {
        partitioningPolicy = PartitioningPolicies.at(tableProfile.GetPartitioningPolicy());
    }

    // Determine used storage policy.
    if (profile.has_storage_policy()) {
        auto &policy = profile.storage_policy();
        if (policy.preset_name()) {
            if (!StoragePolicies.contains(policy.preset_name())) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf("unknown storage policy preset '%s'", policy.preset_name().data());
                return false;
            }
            storagePolicy = StoragePolicies.at(policy.preset_name());
        } else if (StoragePolicies.contains(tableProfile.GetStoragePolicy())) {
            storagePolicy = StoragePolicies.at(tableProfile.GetStoragePolicy());
        }
        // Apply overwritten storage settings for syslog.
        if (policy.has_syslog()) {
            auto &settings = *GetDefaultStorageConfig(storagePolicy)->MutableSysLog();
            settings.SetPreferredPoolKind(policy.syslog().media());
            settings.SetAllowOtherKinds(false);
        }
        // Apply overwritten storage settings for log.
        if (policy.has_log()) {
            auto &settings = *GetDefaultStorageConfig(storagePolicy)->MutableLog();
            settings.SetPreferredPoolKind(policy.log().media());
            settings.SetAllowOtherKinds(false);
        }
        // Apply overwritten storage settings for data.
        if (policy.has_data()) {
            auto &settings = *GetDefaultStorageConfig(storagePolicy)->MutableData();
            settings.SetPreferredPoolKind(policy.data().media());
            settings.SetAllowOtherKinds(false);
        }
        // Apply overwritten storage settings for external.
        if (policy.has_external()) {
            auto &settings = *GetDefaultStorageConfig(storagePolicy)->MutableExternal();
            settings.SetPreferredPoolKind(policy.external().media());
            settings.SetAllowOtherKinds(false);
        }
        switch (policy.keep_in_memory()) {
        case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
            break;
        case Ydb::FeatureFlag::ENABLED:
            if (!AppData()->FeatureFlags.GetEnablePublicApiKeepInMemory()) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = "Setting keep_in_memory to ENABLED is not allowed";
                return false;
            }
            GetDefaultFamilyDescription(storagePolicy)->SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
            break;
        case Ydb::FeatureFlag::DISABLED:
            GetDefaultFamilyDescription(storagePolicy)->ClearColumnCache();
            break;
        default:
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("unknown keep-in-memory featrure flag status %" PRIu32,
                            (ui32)policy.keep_in_memory());
            return false;
        }

        // Perform the same overrides for each named policy there is
        for (auto& family : policy.column_families()) {
            const TString& name = family.name();
            if (name.empty()) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = "Cannot override settings for a family without a name";
                return false;
            }

            auto& familyProto = (name == "default")
                ? *GetDefaultFamilyDescription(storagePolicy)
                : *GetNamedFamilyDescription(storagePolicy, name);

            // Make sure schemeshard has the correct family name specified
            if (!familyProto.HasName()) {
                familyProto.SetName(name);
            }

            if (family.has_data()) {
                auto& settings = *familyProto.MutableStorageConfig()->MutableData();
                settings.SetPreferredPoolKind(family.data().media());
                settings.SetAllowOtherKinds(false);
            }

            if (family.has_external()) {
                auto& settings = *familyProto.MutableStorageConfig()->MutableExternal();
                settings.SetPreferredPoolKind(family.external().media());
                settings.SetAllowOtherKinds(false);
            }

            switch (family.keep_in_memory()) {
            case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
                break;
            case Ydb::FeatureFlag::ENABLED:
                if (!AppData()->FeatureFlags.GetEnablePublicApiKeepInMemory()) {
                    code = Ydb::StatusIds::BAD_REQUEST;
                    error = "Setting keep_in_memory to ENABLED is not allowed";
                    return false;
                }
                familyProto.SetColumnCache(NKikimrSchemeOp::ColumnCacheEver);
                break;
            case Ydb::FeatureFlag::DISABLED:
                familyProto.ClearColumnCache();
                break;
            default:
                code = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf("unknown keep-in-memory featrure flag status %" PRIu32,
                                (ui32)family.keep_in_memory());
                return false;
            }

            switch (family.compression()) {
            case Ydb::Table::ColumnFamilyPolicy::COMPRESSION_UNSPECIFIED:
                break;
            case Ydb::Table::ColumnFamilyPolicy::UNCOMPRESSED:
                familyProto.SetColumnCodec(NKikimrSchemeOp::ColumnCodecPlain);
                break;
            case Ydb::Table::ColumnFamilyPolicy::COMPRESSED:
                familyProto.SetColumnCodec(NKikimrSchemeOp::ColumnCodecLZ4);
                break;
            default:
                code = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf("unknown compression value %" PRIu32,
                                (ui32)family.compression());
                return false;
            }
        }
    } else if (StoragePolicies.contains(tableProfile.GetStoragePolicy())) {
        storagePolicy = StoragePolicies.at(tableProfile.GetStoragePolicy());
    }

    // Determine used replication policy.
    if (profile.has_replication_policy()) {
        auto &policy = profile.replication_policy();
        if (policy.preset_name()) {
            if (!ReplicationPolicies.contains(policy.preset_name())) {
                code = Ydb::StatusIds::BAD_REQUEST;
                error = Sprintf("unknown replciation policy preset '%s'", policy.preset_name().data());
                return false;
            }
            replicationPolicy = ReplicationPolicies.at(policy.preset_name());
        } else if (ReplicationPolicies.contains(tableProfile.GetReplicationPolicy())) {
            replicationPolicy = ReplicationPolicies.at(tableProfile.GetReplicationPolicy());
        }
        if (policy.replicas_count())
            replicationPolicy.SetFollowerCount(policy.replicas_count());
        switch (policy.create_per_availability_zone()) {
        case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
            break;
        case Ydb::FeatureFlag::ENABLED:
            replicationPolicy.SetCrossDataCenter(true);
            break;
        case Ydb::FeatureFlag::DISABLED:
            replicationPolicy.SetCrossDataCenter(false);
            break;
        default:
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("unknown create_per_availability_zone featrure flag status %" PRIu32,
                            (ui32)policy.create_per_availability_zone());
            return false;
        }
        switch (policy.allow_promotion()) {
        case Ydb::FeatureFlag::STATUS_UNSPECIFIED:
            break;
        case Ydb::FeatureFlag::ENABLED:
            replicationPolicy.SetAllowFollowerPromotion(true);
            break;
        case Ydb::FeatureFlag::DISABLED:
            replicationPolicy.SetAllowFollowerPromotion(false);
            break;
        default:
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("unknown allow_promotion featrure flag status %" PRIu32,
                            (ui32)policy.allow_promotion());
            return false;
        }
    } else if (ReplicationPolicies.contains(tableProfile.GetReplicationPolicy())) {
        replicationPolicy = ReplicationPolicies.at(tableProfile.GetReplicationPolicy());
    }

    // Determine used caching policy.
    if (profile.has_caching_policy()) {
        auto &policy = profile.caching_policy();
        if (!CachingPolicies.contains(policy.preset_name())) {
            code = Ydb::StatusIds::BAD_REQUEST;
            error = Sprintf("unknown caching policy preset '%s'", policy.preset_name().data());
            return false;
        }
        cachingPolicy = CachingPolicies.at(policy.preset_name());
    } else if (CachingPolicies.contains(tableProfile.GetCachingPolicy())) {
        cachingPolicy = CachingPolicies.at(tableProfile.GetCachingPolicy());
    }

    // Apply compaction policy to table description.
    if (compactionPolicy.HasCompactionPolicy())
        partitionConfig.MutableCompactionPolicy()->CopyFrom(compactionPolicy.GetCompactionPolicy());

    // Apply execution policy.
    if (executionPolicy.HasPipelineConfig())
        partitionConfig.MutablePipelineConfig()->CopyFrom(executionPolicy.GetPipelineConfig());
    if (executionPolicy.HasResourceProfile())
        partitionConfig.SetResourceProfile(executionPolicy.GetResourceProfile());
    if (executionPolicy.HasEnableFilterByKey())
        partitionConfig.SetEnableFilterByKey(executionPolicy.GetEnableFilterByKey());
    if (executionPolicy.HasExecutorFastLogPolicy())
        partitionConfig.SetExecutorFastLogPolicy(executionPolicy.GetExecutorFastLogPolicy());
    if (executionPolicy.HasTxReadSizeLimit())
        partitionConfig.SetTxReadSizeLimit(executionPolicy.GetTxReadSizeLimit());

    if (executionPolicy.HasEnableEraseCache())
        partitionConfig.SetEnableEraseCache(executionPolicy.GetEnableEraseCache());
    if (executionPolicy.HasEraseCacheMinRows())
        partitionConfig.SetEraseCacheMinRows(executionPolicy.GetEraseCacheMinRows());
    if (executionPolicy.HasEraseCacheMaxBytes())
        partitionConfig.SetEraseCacheMaxBytes(executionPolicy.GetEraseCacheMaxBytes());

    // Apply partitioning policy.
    if (partitioningPolicy.HasUniformPartitionsCount())
        tableDesc.SetUniformPartitionsCount(partitioningPolicy.GetUniformPartitionsCount());
    if (partitioningPolicy.GetAutoSplit()) {
        auto &policy = *partitionConfig.MutablePartitioningPolicy();
        policy.SetSizeToSplit(partitioningPolicy.GetSizeToSplit());
        if (!policy.GetSizeToSplit())
            policy.SetSizeToSplit(1 << 30);
        if (partitioningPolicy.HasMaxPartitionsCount())
            policy.SetMaxPartitionsCount(partitioningPolicy.GetMaxPartitionsCount());
        if (partitioningPolicy.GetAutoMerge()) {
            policy.SetMinPartitionsCount(1);
            if (policy.GetMinPartitionsCount() < partitioningPolicy.GetUniformPartitionsCount())
                policy.SetMinPartitionsCount(partitioningPolicy.GetUniformPartitionsCount());
            if (policy.GetMinPartitionsCount() < (ui32)tableDesc.GetSplitBoundary().size() + 1)
                policy.SetMinPartitionsCount(tableDesc.GetSplitBoundary().size() + 1);
        }
    }

    // Apply storage config.
    for (auto &family : storagePolicy.GetColumnFamilies())
        partitionConfig.AddColumnFamilies()->CopyFrom(family);

    // Apply replication policy.
    if (replicationPolicy.GetFollowerCount()) {
        auto& followerGroup = *partitionConfig.AddFollowerGroups();
        followerGroup.SetFollowerCount(replicationPolicy.GetFollowerCount());
        if (replicationPolicy.GetCrossDataCenter())
            followerGroup.SetRequireAllDataCenters(true);
        else
            followerGroup.SetRequireAllDataCenters(false);
        if (replicationPolicy.HasAllowFollowerPromotion()) {
            followerGroup.SetAllowLeaderPromotion(replicationPolicy.GetAllowFollowerPromotion());
        }
    }

    if (cachingPolicy.HasExecutorCacheSize())
        partitionConfig.SetExecutorCacheSize(cachingPolicy.GetExecutorCacheSize());

    return true;
}

bool TTableProfiles::ApplyCompactionPolicy(const TString &name,
    NKikimrSchemeOp::TPartitionConfig &partitionConfig, Ydb::StatusIds::StatusCode &code,
    TString &error, const TAppData* appData) const
{
    NKikimrConfig::TCompactionPolicy compactionPolicy;

    if (!CompactionPolicies.contains(name)) {
        code = Ydb::StatusIds::BAD_REQUEST;
        error = Sprintf("unknown compaction policy preset '%s'", name.c_str());
        return false;
    }
    compactionPolicy = CompactionPolicies.at(name);
    // Apply compaction policy to table description.
    if (compactionPolicy.HasCompactionPolicy()) {
        partitionConfig.MutableCompactionPolicy()->CopyFrom(compactionPolicy.GetCompactionPolicy());
    } else if (appData) {
        TIntrusiveConstPtr<NLocalDb::TCompactionPolicy> defaultPolicy = appData->DomainsInfo->GetDefaultUserTablePolicy();
        defaultPolicy->Serialize(*partitionConfig.MutableCompactionPolicy());
    }
    return true;
}


} // namespace NKikimr
