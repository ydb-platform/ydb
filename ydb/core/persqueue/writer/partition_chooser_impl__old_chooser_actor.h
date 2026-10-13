#pragma once

#include "partition_chooser_impl__abstract_chooser_actor.h"
#include "partition_chooser_impl__pqrb_helper.h"

namespace NKikimr::NPQ::NPartitionChooser {

#if defined(LOG_PREFIX)
#error "Already defined LOG_PREFIX"
#endif


#define LOG_PREFIX TStringBuilder() << "TPartitionChooser " << SelfId()                         \
                    << " (SourceId=" << TThis::SourceId                     \
                    << ", PreferedPartition=" << TThis::PreferedPartition   \
                    << ") "

template<typename TPipeCreator>
class TPartitionChooserActor: public TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator> {
public:
    using TThis = TPartitionChooserActor<TPipeCreator>;
    using TThisActor = TActor<TThis>;
    using TParentActor = TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator>;

    TPartitionChooserActor(TActorId parentId,
                           const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                           const std::shared_ptr<IPartitionChooser>& chooser,
                           NPersQueue::TTopicConverterPtr& fullConverter,
                           const TString& sourceId,
                           std::optional<ui32> preferedPartition,
                           NWilson::TTraceId traceId)
        : TAbstractPartitionChooserActor<TPartitionChooserActor<TPipeCreator>, TPipeCreator>(parentId, chooser, fullConverter, sourceId, preferedPartition, std::move(traceId))
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
        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "RequestPQRB",
            {"logPrefix", LOG_PREFIX});
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
        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "Received partition from PQRB",
            {"logPrefix", LOG_PREFIX},
            {"partitionId", PartitionId},
            {"sourceId", TThis::SourceId});
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
        YDB_LOG_TRACE_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "OnPartitionChosen",
            {"logPrefix", LOG_PREFIX});

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

} // namespace NKikimr::NPQ::NPartitionChooser
