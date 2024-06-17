#pragma once

#include <ydb/core/base/path.h>
#include "ydb/core/persqueue/utils.h"
#include <ydb/core/persqueue/partition_scale_request.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <util/system/types.h>
#include <util/generic/fwd.h>
#include <util/generic/string.h>

#include <unordered_map>
#include <utility>

namespace NKikimr {
namespace NPQ {

class TPartitionScaleManager {
private:
    struct TBalancerConfig {
        TBalancerConfig(
            ui64 pathId,
            int version,
            const NKikimrPQ::TPQTabletConfig& config
        )
            : PathId(pathId)
            , PathVersion(version)
            , PartitionGraph(MakePartitionGraph(config))
            , MaxActivePartitions(config.GetPartitionStrategy().GetMaxPartitionCount())
            , MinActivePartitions(config.GetPartitionStrategy().GetMinPartitionCount())
            , CurPartitions(std::count_if(config.GetAllPartitions().begin(), config.GetAllPartitions().end(), [](auto& p) {
                return p.GetStatus() != NKikimrPQ::ETopicPartitionStatus::Inactive;
            })) {
        }

        ui64 PathId;
        int PathVersion;
        TPartitionGraph PartitionGraph;
        ui64 MaxActivePartitions;
        ui64 MinActivePartitions;
        ui64 CurPartitions;
    };

public:
    TPartitionScaleManager(const TString& topicPath, const TString& databasePath, ui64 pathId, int version, const NKikimrPQ::TPQTabletConfig& config);

public:
    void HandleScaleStatusChange(const ui32 partition, NKikimrPQ::EScaleStatus scaleStatus, const TActorContext& ctx);
    void HandleScaleRequestResult(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx);
    void TrySendScaleRequest(const TActorContext& ctx);
    void UpdateBalancerConfig(ui64 pathId, int version, const NKikimrPQ::TPQTabletConfig& config);
    void UpdateDatabasePath(const TString& dbPath);
    void Die(const TActorContext& ctx);

private:
    using TPartitionSplit = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit;
    using TPartitionMerge = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge;

    std::pair<std::vector<TPartitionSplit>, std::vector<TPartitionMerge>> BuildScaleRequest(const TActorContext& ctx);

public:
    static const ui64 TRY_SCALE_REQUEST_WAKE_UP_TAG = 10;

private:
    static const ui32 MIN_SCALE_REQUEST_REPEAT_SECONDS_TIMEOUT = 10;
    static const ui32 MAX_SCALE_REQUEST_REPEAT_SECONDS_TIMEOUT = 1000;

    const TString TopicName;
    TString DatabasePath = "";
    TActorId CurrentScaleRequest;
    TDuration RequestTimeout = TDuration::MilliSeconds(0);
    TInstant LastResponseTime = TInstant::Zero();

    std::unordered_set<ui32> PartitionsToSplit;

    TBalancerConfig BalancerConfig;
    bool RequestInflight = false;
};

} // namespace NPQ
} // namespace NKikimr
