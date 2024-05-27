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

public:
    struct TPartitionInfo {
        ui32 Id;
        NSchemeShard::TTopicTabletInfo::TKeyRange KeyRange;
    };

private:
    struct TBalancerConfig {
        TBalancerConfig(
            NKikimrPQ::TUpdateBalancerConfig& config
        )
            : PathId(config.GetPathId())
            , PathVersion(config.GetVersion())
            , PartitionGraph(MakePartitionGraph(config))
            , MaxActivePartitions(config.GetTabletConfig().GetPartitionStrategy().GetMaxPartitionCount())
            , MinActivePartitions(config.GetTabletConfig().GetPartitionStrategy().GetMinPartitionCount())
            , CurPartitions(config.PartitionsSize()) {
        }

        ui64 PathId;
        int PathVersion;
        TPartitionGraph PartitionGraph;
        ui64 MaxActivePartitions;
        ui64 MinActivePartitions;
        ui64 CurPartitions;
    };

public:
    TPartitionScaleManager(const TString& topicPath, const TString& databasePath, NKikimrPQ::TUpdateBalancerConfig& balancerConfig);

public:
    void HandleScaleStatusChange(const TPartitionInfo& partition, NKikimrPQ::EScaleStatus scaleStatus, const TActorContext& ctx);
    void HandleScaleRequestResult(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx);
    void TrySendScaleRequest(const TActorContext& ctx);
    void UpdateBalancerConfig(NKikimrPQ::TUpdateBalancerConfig& config);
    void UpdateDatabasePath(const TString& dbPath);
    void Die(const TActorContext& ctx);

    static TString GetRangeMid(const TString& from, const TString& to);

private:
    using TPartitionSplit = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit;
    using TPartitionMerge = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge;

    std::pair<std::vector<TPartitionSplit>, std::vector<TPartitionMerge>> BuildScaleRequest();

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

    std::unordered_map<ui64, TPartitionInfo> PartitionsToSplit;

    TBalancerConfig BalancerConfig;
    bool RequestInflight = false;
};

} // namespace NPQ
} // namespace NKikimr
