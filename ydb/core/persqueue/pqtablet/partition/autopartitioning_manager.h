#pragma once

#include <ydb/core/persqueue/common/partitioning_keys_manager.h>
#include <ydb/core/persqueue/common/write_stats.h>
#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <library/cpp/sliding_window/sliding_window.h>

#include <util/generic/fwd.h>



namespace NKikimr::NPQ {

class TPartitionId;

using TUint128 = unsigned __int128;

struct TAutopartitioningManagerSnapshot {
    std::unordered_map<TString, std::vector<std::pair<TString, ui64>>> PerSourceMetrics;
    std::unordered_map<TString, NPQ::TPartitioningKeysManager> KeysManagers;
};

class IAutopartitioningManager {
public:
    virtual ~IAutopartitioningManager() = default;

    virtual void OnWrite(const TString& sourceId, ui64 bytes, ui64 messages, const TString& key = "") = 0;
    virtual void AddKeysStats(const TString& tag, const NPQ::TPartitioningKeysManager& keysManager) = 0;
    virtual void CleanUp() = 0;

    virtual NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus currentState) = 0;
    virtual std::optional<TString> SplitBoundary() = 0;
    virtual TAutopartitioningManagerSnapshot GetSnapshot() = 0;

    virtual void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) = 0;
};

IAutopartitioningManager* CreateAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, const TPartitionId& partitionId);

}
