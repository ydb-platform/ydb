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
        , Graph(config.GetPQTabletConfig()) {

    }

    void Bootstrap(const TActorContext& ctx) {
        BoundaryPartition = ChoosePartitionSync(ctx);

        TThis::Initialize(ctx);
        TThis::InitTable(ctx);
    }

    TActorIdentity SelfId() const {
        return TActor<TSMPartitionChooserActor<TPipeCreator>>::SelfId();
    }

    void OnSelected(const TActorContext &ctx) override {
        OldPartition = TThis::Partition;

        if (BoundaryPartition == TThis::Partition) {
            return OnPartitionChosen(ctx);
        }

        if (!TThis::TableHelper.PartitionId() || !TThis::Partition) {
            TThis::Partition = BoundaryPartition;
            return OnPartitionChosen(ctx);
        }

        auto activeChildren = Graph.GetActiveChildren(TThis::TableHelper.PartitionId().value());
        if (activeChildren.empty()) {
            return TThis::ReplyError(ErrorCode::ERROR, TStringBuilder() << "has't active partition Marker# PC01", ctx);
        }
        if (activeChildren.contains(BoundaryPartition->PartitionId)) {
            TThis::Partition = BoundaryPartition;
        } else {
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

    void OnOwnership(const TActorContext &ctx) override {
        DEBUG("OnOwnership");
        TThis::SendUpdateRequests(ctx);
    }

private:
    void GetOldSeqNo(const TActorContext &ctx) {
        DEBUG("GetOldSeqNo");
        TThis::Become(&TThis::StateGetMaxSeqNo);

        if (!OldPartition) {
            return TThis::ReplyError(ErrorCode::ERROR, TStringBuilder() << "Inconsistent status Marker# PC03", ctx);
        }

        TThis::PartitionHelper.Open(OldPartition->TabletId, ctx);
        TThis::PartitionHelper.SendMaxSeqNoRequest(OldPartition->PartitionId, TThis::SourceId, ctx);
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
            TThis::SeqNo = 0; // TODO from table
            break;
        }

        TThis::PartitionHelper.Close(ctx);
        TThis::StartGetOwnership(ctx);
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

        TThis::StartGetOwnership(ctx);
    }

    const TPartitionInfo* ChoosePartitionSync(const TActorContext& /*ctx*/) const {
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
    const TPartitionInfo* OldPartition = nullptr;
    const TPartitionGraph Graph;
};

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

} // namespace NKikimr::NPQ::NPartitionChooser
