#pragma once

#include "partition_chooser_impl__abstract_chooser_actor.h"

#include <ydb/core/persqueue/utils.h>

namespace NKikimr::NPQ::NPartitionChooser {

#if defined(LOG_PREFIX) || defined(TRACE) || defined(DEBUG) || defined(INFO) || defined(ERROR)
#error "Already defined LOG_PREFIX or TRACE or DEBUG or INFO or ERROR"
#endif


#define LOG_PREFIX "TPartitionChooser " << SelfId()                         \
                    << " (SourceId=" << TThis::SourceId                     \
                    << ", PreferedPartition=" << TThis::PreferedPartition   \
                    << ") "
#define TRACE(message) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define INFO(message)  LOG_INFO_S (*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);

template<typename TPipeCreator>
class TSMPartitionChooserActor: public TAbstractPartitionChooserActor<TSMPartitionChooserActor<TPipeCreator>, TPipeCreator> {
public:
    using TThis = TSMPartitionChooserActor<TPipeCreator>;
    using TThisActor = TActor<TThis>;
    using TParentActor = TAbstractPartitionChooserActor<TSMPartitionChooserActor<TPipeCreator>, TPipeCreator>;

    TSMPartitionChooserActor(TActorId parentId,
                           const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                           std::shared_ptr<IPartitionChooser>& chooser,
                           NPersQueue::TTopicConverterPtr& fullConverter,
                           const TString& sourceId,
                           std::optional<ui32> preferedPartition)
        : TAbstractPartitionChooserActor<TSMPartitionChooserActor<TPipeCreator>, TPipeCreator>(parentId, chooser, fullConverter, sourceId, preferedPartition)
        , Graph(MakePartitionGraph(config)) {
    }

    void Bootstrap(const TActorContext& ctx) {
        if (!TThis::Initialize(ctx)) {
            return;
        }
        BoundaryPartition = ChoosePartitionSync();

        if (TThis::SourceId) {
            GetOwnershipFast(ctx);
        } else {
            TThis::Partition = BoundaryPartition;
            TThis::StartCheckPartitionRequest(ctx);
        }
    }

    TActorIdentity SelfId() const {
        return TActor<TSMPartitionChooserActor<TPipeCreator>>::SelfId();
    }

    void OnSelected(const TActorContext &ctx) override {
        if (TThis::Partition) {
            // If we have found a partition, it means that the partition is active, which means
            // that we need to continue writing to it, and it does not matter whether it falls
            // within the boundaries of the distribution.
            return OnPartitionChosen(ctx);
        }

        if (!TThis::TableHelper.PartitionId()) {
            // They didn't write with this SourceId earlier, or the SourceID has already been deleted.
            TThis::Partition = BoundaryPartition;
            return OnPartitionChosen(ctx);
        }

        const auto* node = Graph.GetPartition(TThis::TableHelper.PartitionId().value());
        if (!node) {
            // The partition where the writting was performed earlier has already been deleted.
            // We can write without taking into account the hierarchy of the partition.
            TThis::Partition = BoundaryPartition;
            return OnPartitionChosen(ctx);
        }

        // Choosing a partition based on the split and merge hierarchy.
        auto activeChildren = Graph.GetActiveChildren(TThis::TableHelper.PartitionId().value());
        if (activeChildren.empty()) {
            return TThis::ReplyError(ErrorCode::ERROR, TStringBuilder() << "has't active partition Marker# PC01", ctx);
        }

        if (activeChildren.contains(BoundaryPartition->PartitionId)) {
            // First of all, we are trying to write to the partition taking into account the
            // distribution boundaries.
            TThis::Partition = BoundaryPartition;
        } else {
            // It is important to save the write into account the hierarchy of partitions, because
            // this will preserve the guarantees of the order of reading.
            auto n = RandomNumber<size_t>(activeChildren.size());
            std::vector<ui32> ac;
            ac.reserve(activeChildren.size());
            ac.insert(ac.end(), activeChildren.begin(), activeChildren.end());
            auto id = ac[n];
            TThis::Partition = TThis::Chooser->GetPartition(id);
        }

        if (!TThis::Partition) {
            return TThis::ReplyError(ErrorCode::ERROR, TStringBuilder() << "can't choose partition Marker# PC02", ctx);
        }

        GetOldSeqNo(ctx);
    }

private:
    void GetOwnershipFast(const TActorContext &ctx) {
        TThis::Become(&TThis::StateOwnershipFast);
        if (!BoundaryPartition) {
            return TThis::ReplyError(TThis::PreferedPartition ? ErrorCode::WRITE_ERROR_PARTITION_INACTIVE : ErrorCode::INITIALIZING, "A partition not choosed", ctx);
        }

        DEBUG("GetOwnershipFast Partition=" << BoundaryPartition->PartitionId << " TabletId=" << BoundaryPartition->TabletId);

        TThis::PartitionHelper.Open(BoundaryPartition->TabletId, ctx);
        TThis::PartitionHelper.SendCheckPartitionStatusRequest(BoundaryPartition->PartitionId, TThis::SourceId, ctx);
    }

    void HandleFast(NKikimr::TEvPQ::TEvCheckPartitionStatusResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        TThis::PartitionHelper.Close(ctx);
        if (NKikimrPQ::ETopicPartitionStatus::Active == ev->Get()->Record.GetStatus()
                && ev->Get()->Record.HasSeqNo()
                && ev->Get()->Record.GetSeqNo() > 0) {
            // Fast path: the partition ative and already written
            TThis::Partition = BoundaryPartition;
            TThis::SeqNo = ev->Get()->Record.GetSeqNo();

            TThis::SendUpdateRequests(ctx);
            return TThis::ReplyResult(ctx);
        }

        TThis::InitTable(ctx);
    }

    STATEFN(StateOwnershipFast) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::TEvPQ::TEvCheckPartitionStatusResponse, HandleFast);
            HFunc(TEvTabletPipe::TEvClientConnected, TThis::HandleOwnership);
            HFunc(TEvTabletPipe::TEvClientDestroyed, TThis::HandleOwnership);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }


private:
    void GetOldSeqNo(const TActorContext &ctx) {
        DEBUG("GetOldSeqNo");
        TThis::Become(&TThis::StateGetMaxSeqNo);

        const auto* oldNode = Graph.GetPartition(TThis::TableHelper.PartitionId().value());

        if (!oldNode) {
            return TThis::ReplyError(ErrorCode::ERROR, TStringBuilder() << "Inconsistent status Marker# PC03", ctx);
        }

        TThis::PartitionHelper.Open(oldNode->TabletId, ctx);
        TThis::PartitionHelper.SendMaxSeqNoRequest(oldNode->Id, TThis::SourceId, ctx);
    }

    void HandleMaxSeqNo(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;

        TString error;
        if (!BasicCheck(record, error)) {
            return TThis::ReplyError(ErrorCode::INITIALIZING, std::move(error), ctx);
        }

        const auto& response = record.GetPartitionResponse();
        if (!response.HasCmdGetMaxSeqNoResult()) {
            return TThis::ReplyError(ErrorCode::INITIALIZING, "Absent MaxSeqNo result", ctx);
        }

        const auto& result = response.GetCmdGetMaxSeqNoResult();
        if (result.SourceIdInfoSize() < 1) {
            return TThis::ReplyError(ErrorCode::INITIALIZING, "Empty source id info", ctx);
        }

        const auto& sourceIdInfo = result.GetSourceIdInfo(0);
        switch (sourceIdInfo.GetState()) {
        case NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED:
            TThis::SeqNo = sourceIdInfo.GetSeqNo();
            break;
        case NKikimrPQ::TMessageGroupInfo::STATE_PENDING_REGISTRATION:
        case NKikimrPQ::TMessageGroupInfo::STATE_UNKNOWN:
            TThis::SeqNo = TThis::TableHelper.SeqNo();
            break;
        }

        TThis::PartitionHelper.Close(ctx);
        OnPartitionChosen(ctx);
    }

    STATEFN(StateGetMaxSeqNo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, HandleMaxSeqNo);
            SFunc(TEvents::TEvPoison, TThis::Die);
            HFunc(TEvTabletPipe::TEvClientConnected, TThis::HandleOwnership);
            HFunc(TEvTabletPipe::TEvClientDestroyed, TThis::HandleOwnership);
        }
    }


private:
    void OnPartitionChosen(const TActorContext& ctx) {
        TRACE("OnPartitionChosen");

        if (!TThis::Partition && TThis::PreferedPartition) {
            return TThis::ReplyError(ErrorCode::BAD_REQUEST,
                            TStringBuilder() << "Prefered partition " << (TThis::PreferedPartition.value() + 1) << " is not exists or inactive.",
                            ctx);
        }

        if (!TThis::Partition) {
            return TThis::ReplyError(ErrorCode::INITIALIZING, "Can't choose partition", ctx);
        }

        if (TThis::PreferedPartition && TThis::Partition->PartitionId != TThis::PreferedPartition.value()) {
            return TThis::ReplyError(ErrorCode::BAD_REQUEST,
                            TStringBuilder() << "MessageGroupId " << TThis::SourceId << " is already bound to PartitionGroupId "
                                        << (TThis::Partition->PartitionId + 1) << ", but client provided " << (TThis::PreferedPartition.value() + 1)
                                        << ". MessageGroupId->PartitionGroupId binding cannot be changed, either use "
                                        "another MessageGroupId, specify PartitionGroupId " << (TThis::Partition->PartitionId + 1)
                                        << ", or do not specify PartitionGroupId at all.",
                            ctx);
        }

        TThis::StartCheckPartitionRequest(ctx);
    }

    const TPartitionInfo* ChoosePartitionSync() const {
        if (TThis::PreferedPartition) {
            return TThis::Chooser->GetPartition(TThis::PreferedPartition.value());
        } else if (TThis::SourceId) {
            return TThis::Chooser->GetPartition(TThis::SourceId);
        } else {
            return TThis::Chooser->GetRandomPartition();
        }
    };


private:
    const TPartitionInfo* BoundaryPartition = nullptr;
    const TPartitionGraph Graph;
};

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

} // namespace NKikimr::NPQ::NPartitionChooser
