#pragma once

#include "partition_chooser_impl__abstract_chooser_actor.h"

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
class TSMPartitionChooserActor: public TAbstractPartitionChooserActor<TSMPartitionChooserActor<TPipeCreator>, TPipeCreator> {
public:
    using TThis = TSMPartitionChooserActor<TPipeCreator>;
    using TThisActor = TActor<TThis>;
    using TParentActor = TAbstractPartitionChooserActor<TSMPartitionChooserActor<TPipeCreator>, TPipeCreator>;

    TSMPartitionChooserActor(TActorId parentId,
                           const NKikimrSchemeOp::TPersQueueGroupDescription& /*config*/,
                           std::shared_ptr<IPartitionChooser>& chooser,
                           NPersQueue::TTopicConverterPtr& fullConverter,
                           const TString& sourceId,
                           std::optional<ui32> preferedPartition)
        : TAbstractPartitionChooserActor<TSMPartitionChooserActor<TPipeCreator>, TPipeCreator>(parentId, chooser, fullConverter, sourceId, preferedPartition) {
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
        if (BoundaryPartition == TThis::Partition) {
            return OnPartitionChosen(ctx);
        }

        if (!TThis::TableHelper.PartitionId()) {
            TThis::Partition = BoundaryPartition;
            return OnPartitionChosen(ctx);
        }

        if (!TThis::Partition) {

        }
    }

    void OnOwnership(const TActorContext &ctx) override {
        DEBUG("OnOwnership");
        TThis::SendUpdateRequests(ctx);
    }

private:


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
};

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

} // namespace NKikimr::NPQ::NPartitionChooser
