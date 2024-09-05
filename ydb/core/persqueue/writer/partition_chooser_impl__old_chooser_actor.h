#pragma once

#include "partition_chooser_impl__abstract_chooser_actor.h"
#include "partition_chooser_impl__pqrb_helper.h"

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
#define INFO(message)  LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);

template<typename TPipeCreator>
class TPartitionChooserActor: public TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator> {
public:
    using TThis = TPartitionChooserActor<TPipeCreator>;
    using TThisActor = TActor<TThis>;
    using TParentActor = TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator>;

    TPartitionChooserActor(TActorId parentId,
                           const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                           std::shared_ptr<IPartitionChooser>& chooser,
                           NPersQueue::TTopicConverterPtr& fullConverter,
                           const TString& sourceId,
                           std::optional<ui32> preferedPartition)
        : TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator>(parentId, chooser, fullConverter, sourceId, preferedPartition)
        , PQRBHelper(config.GetBalancerTabletID()) {
    }

    void Bootstrap(const TActorContext& ctx) {
        if (!TThis::Initialize(ctx)) {
            return;
        }
        TThis::InitTable(ctx);
    }

    TActorIdentity SelfId() const {
        return TActor<TPartitionChooserActor<TPipeCreator>>::SelfId();
    }

    void OnSelected(const TActorContext &ctx) override {
        if (TThis::Partition) {
            return OnPartitionChosen(ctx);
        }

        auto [roundRobin, p] = ChoosePartitionSync(ctx);
        if (roundRobin) {
            RequestPQRB(ctx);
        } else {
            TThis::Partition = p;
            OnPartitionChosen(ctx);
        }
    }

private:
    void RequestPQRB(const NActors::TActorContext& ctx) {
        DEBUG("RequestPQRB")
        TThis::Become(&TThis::StatePQRB);

        if (PQRBHelper.PartitionId()) {
            PartitionId = PQRBHelper.PartitionId();
            OnPartitionChosen(ctx);
        } else {
            PQRBHelper.SendRequest(ctx);
        }
    }

    void Handle(TEvPersQueue::TEvGetPartitionIdForWriteResponse::TPtr& ev, const TActorContext& ctx) {
        PartitionId = PQRBHelper.Handle(ev, ctx);
        DEBUG("Received partition " << PartitionId << " from PQRB for SourceId=" << TThis::SourceId);
        TThis::Partition = TThis::Chooser->GetPartition(PQRBHelper.PartitionId().value());

        PQRBHelper.Close(ctx);

        OnPartitionChosen(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx) {
        if (PQRBHelper.IsPipe(ev->Sender) && ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
            TThis::ReplyError(ErrorCode::INITIALIZING, "Pipe connection fail", ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx) {
        if(PQRBHelper.IsPipe(ev->Sender)) {
            TThis::ReplyError(ErrorCode::INITIALIZING, "Pipe destroyed", ctx);
        }
    }

    STATEFN(StatePQRB) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvGetPartitionIdForWriteResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            SFunc(TEvents::TEvPoison, TThis::Die);
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

        TThis::SendUpdateRequests(ctx);
    }

    std::pair<bool, const TPartitionInfo*> ChoosePartitionSync(const TActorContext& ctx) const {
        const auto& pqConfig = AppData(ctx)->PQConfig;
        if (TThis::PreferedPartition) {
            return {false, TThis::Chooser->GetPartition(TThis::PreferedPartition.value())};
        } else if (pqConfig.GetTopicsAreFirstClassCitizen() && TThis::SourceId) {
            return {false, TThis::Chooser->GetPartition(TThis::SourceId)};
        } else {
            return {true, nullptr};
        }
    };


private:
    std::optional<ui32> PartitionId;

    TPQRBHelper<TPipeCreator> PQRBHelper;
};

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

} // namespace NKikimr::NPQ::NPartitionChooser
