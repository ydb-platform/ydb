#pragma once

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <util/generic/hash.h>

namespace NKikimr {

class TTableProfiles {
public:
    TTableProfiles();
    TTableProfiles(const NKikimrConfig::TTableProfilesConfig &config);

    void Load(const NKikimrConfig::TTableProfilesConfig &config);

    bool HasPresetName(const TString& presetName) const;

    bool ApplyTableProfile(const Ydb::Table::TableProfile &profile,
                           NKikimrSchemeOp::TTableDescription &tableDesc,
                           Ydb::StatusIds::StatusCode &code,
                           TString &error) const;

    bool ApplyCompactionPolicy(const TString &name, NKikimrSchemeOp::TPartitionConfig &partitionConfig,
        Ydb::StatusIds::StatusCode &code, TString &error, const TAppData* appData = nullptr) const;

private:
    NKikimrSchemeOp::TFamilyDescription *GetNamedFamilyDescription(NKikimrConfig::TStoragePolicy &policy, const TString& name) const;
    NKikimrSchemeOp::TFamilyDescription *GetDefaultFamilyDescription(NKikimrConfig::TStoragePolicy &policy) const;
    NKikimrSchemeOp::TStorageConfig *GetDefaultStorageConfig(NKikimrConfig::TStoragePolicy &policy) const;
public:
    THashMap<TString, NKikimrConfig::TCompactionPolicy> CompactionPolicies;
    THashMap<TString, NKikimrConfig::TExecutionPolicy> ExecutionPolicies;
    THashMap<TString, NKikimrConfig::TPartitioningPolicy> PartitioningPolicies;
    THashMap<TString, NKikimrConfig::TStoragePolicy> StoragePolicies;
    THashMap<TString, NKikimrConfig::TReplicationPolicy> ReplicationPolicies;
    THashMap<TString, NKikimrConfig::TCachingPolicy> CachingPolicies;
    THashMap<TString, NKikimrConfig::TTableProfile> TableProfiles;
};

} // namespace NKikimr
