#pragma once

#include <ydb/core/persqueue/common/partitioning_keys_manager.h>
#include <ydb/core/persqueue/common/write_stats.h>
#include <ydb/core/persqueue/public/config.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>

#include <library/cpp/sliding_window/sliding_window.h>

#include <memory>

#include <util/generic/fwd.h>

namespace NKikimr::NPQ {

class TPartitionId;

using TUint128 = unsigned __int128;

struct TAutopartitioningManagerSnapshot {
    std::vector<std::vector<std::pair<TString, ui64>>> PerSourceMetrics;
    std::vector<std::unique_ptr<NPQ::TPartitioningKeysManager>> KeysManagers;
};

enum class ETag : ui8 {
    BYTES = 0,
    MESSAGES,
    Count,
};

inline constexpr size_t AutoscaleMetricTagCount = static_cast<size_t>(ETag::Count);

class IAutopartitioningManager {
public:
    virtual ~IAutopartitioningManager() = default;

    virtual void OnWrite(const TString& sourceId, ui64 bytes, ui64 messages, const TString& key = "") = 0;
    virtual void AddKeysStats(ETag tag, const NPQ::TPartitioningKeysManager& keysManager) = 0;
    virtual void CleanUp() = 0;

    virtual NKikimrPQ::EScaleStatus GetScaleStatus(NKikimrPQ::EScaleStatus currentState) = 0;
    virtual std::optional<TString> SplitBoundary() = 0;
    virtual TAutopartitioningManagerSnapshot GetSnapshot() = 0;

    virtual void UpdateConfig(const NKikimrPQ::TPQTabletConfig& config) = 0;
};

IAutopartitioningManager* CreateAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, const TPartitionId& partitionId);

}
