#pragma once

#include "partition_scale_request.h"

#include "partition_scale_manager_graph_cmp.h"

#include <ydb/core/base/path.h>
#include "ydb/core/persqueue/public/utils.h"
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/util/backoff.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/system/types.h>
#include <util/generic/fwd.h>
#include <util/generic/string.h>

#include <map>
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
            , MaxActivePartitions(config.GetPartitionStrategy().GetMaxPartitionCount())
            , MinActivePartitions(config.GetPartitionStrategy().GetMinPartitionCount())
            , CurPartitions(std::count_if(config.GetAllPartitions().begin(), config.GetAllPartitions().end(), [](auto& p) {
                return p.GetStatus() != NKikimrPQ::ETopicPartitionStatus::Inactive;
            })) {
        }

        ui64 PathId;
        int PathVersion;
        ui64 MaxActivePartitions;
        ui64 MinActivePartitions;
        ui64 CurPartitions;
    };

    struct TPartitionScaleOperationInfo {
        ui32 PartitionId{};
        TMaybe<NKikimrPQ::TPartitionScaleParticipants> PartitionScaleParticipants;
        TMaybe<TString> SplitBoundary;
    };

    struct TBuildSplitScaleRequestResult;

public:
    TPartitionScaleManager(const TString& topicName, const TString& topicPath, const TString& databasePath, ui64 pathId, int version,
        const NKikimrPQ::TPQTabletConfig& config, const TPartitionGraph& partitionGraph);

public:
    void HandleScaleStatusChange(const ui32 partition, NKikimrPQ::EScaleStatus scaleStatus,
        TMaybe<NKikimrPQ::TPartitionScaleParticipants> participants,
        TMaybe<TString> splitBoundary,
        const TActorContext& ctx);
    void HandleScaleRequestResult(TPartitionScaleRequest::TEvPartitionScaleRequestDone::TPtr& ev, const TActorContext& ctx);
    std::expected<void, std::string> HandleMirrorTopicDescriptionResult(TEvPQ::TEvMirrorTopicDescription::TPtr& ev, const TActorContext& ctx);

    void TrySendScaleRequest(const TActorContext& ctx);
    void UpdateBalancerConfig(ui64 pathId, int version, const NKikimrPQ::TPQTabletConfig& config);
    void UpdateDatabasePath(const TString& dbPath);
    void Die(const TActorContext& ctx);

private:
    using TPartitionSplit = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionSplit;
    using TPartitionMerge = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionMerge;
    using TPartitionBoundary = NKikimrSchemeOp::TPersQueueGroupDescription_TPartitionBoundary;
    using TPartitionsToSplitMap = std::map<ui32, TPartitionScaleOperationInfo>;

    class TScaleRequest {
    public:
        std::vector<TPartitionSplit> Split;
        std::vector<TPartitionMerge> Merge;
        std::vector<TPartitionBoundary> SetBoundary;
    public:
        bool Empty() const {
            return Split.empty() && Merge.empty() && SetBoundary.empty();
        }
    };

    template <class T>
    struct TRequests {
        std::vector<T> Requests;
        size_t Unprocessed = 0;

        bool Empty() const {
            return Requests.empty();
        }
    };

    TScaleRequest BuildScaleRequest(const TActorContext& ctx);
    TRequests<TPartitionBoundary> BuildSetBoundaryRequest(size_t& allowedSplitsCount);
    TRequests<TPartitionSplit> BuildSplitRequest(size_t& allowedSplitsCount);
    TRequests<TPartitionMerge> BuildMergeRequest(size_t& allowedSplitsCount);
    TBuildSplitScaleRequestResult BuildSplitScaleRequest(const TPartitionScaleOperationInfo& splitParameters) const;
    std::vector<TPartitionsToSplitMap::const_iterator> ReorderSplits() const;
    TString LogPrefix() const;
    void ClearMirrorInfo();
    void UpdateMirrorRootPartitionsSet();

public:
    static const ui64 TRY_SCALE_REQUEST_WAKE_UP_TAG = 10;

private:
    const TString TopicName;
    const TString TopicPath;
    TString DatabasePath = "";
    TActorId CurrentScaleRequest;
    TBackoff Backoff = TBackoff(TDuration::Seconds(1), TDuration::Minutes(15));
    TDuration RequestTimeout;
    TInstant LastResponseTime = TInstant::Zero();

    TPartitionsToSplitMap PartitionsToSplit;

    std::optional<NYdb::NTopic::TTopicDescription> MirrorTopicDescription;
    std::optional<std::vector<NMirror::TPartitionWithBounds>> RootPartitionsToCreate;
    std::optional<TString> MirrorTopicError;

    TBalancerConfig BalancerConfig;
    const TPartitionGraph& PartitionGraph;

    bool RequestInflight = false;
    bool MirroredFromSomewhere = false;
    bool RootPartitionsResetRequestInflight = false;
};

} // namespace NPQ
} // namespace NKikimr
