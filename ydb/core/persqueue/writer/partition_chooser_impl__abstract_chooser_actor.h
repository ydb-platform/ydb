#pragma once

#include "partition_chooser.h"
#include "partition_chooser_impl__partition_helper.h"
#include "partition_chooser_impl__table_helper.h"

#include <ydb/core/persqueue/pq_database.h>
#include <ydb/core/persqueue/writer/metadata_initializers.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NPQ::NPartitionChooser {

#if defined(LOG_PREFIX) || defined(TRACE) || defined(DEBUG) || defined(INFO) || defined(ERROR)
#error "Already defined LOG_PREFIX or TRACE or DEBUG or INFO or ERROR"
#endif


#define LOG_PREFIX "TPartitionChooser " << SelfId()                  \
                    << " (SourceId=" << SourceId                     \
                    << ", PreferedPartition=" << PreferedPartition   \
                    << ") "
#define TRACE(message) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define DEBUG(message) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define INFO(message)  LOG_INFO_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);
#define ERROR(message) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::PQ_PARTITION_CHOOSER, LOG_PREFIX << message);

using TPartitionInfo = typename IPartitionChooser::TPartitionInfo;

using namespace NActors;
using namespace NSourceIdEncoding;
using namespace Ydb::PersQueue::ErrorCode;

template<typename TDerived, typename TPipeCreator>
class TAbstractPartitionChooserActor: public TActorBootstrapped<TDerived> {
public:
    using TThis = TAbstractPartitionChooserActor<TDerived, TPipeCreator>;
    using TThisActor = TActor<TThis>;


    TAbstractPartitionChooserActor(TActorId parentId,
                                   std::shared_ptr<IPartitionChooser>& chooser,
                                   NPersQueue::TTopicConverterPtr& fullConverter,
                                   const TString& sourceId,
                                   std::optional<ui32> preferedPartition)
        : Parent(parentId)
        , SourceId(sourceId)
        , PreferedPartition(preferedPartition)
        , Chooser(chooser)
        , TableHelper(fullConverter->GetClientsideName(), fullConverter->GetTopicForSrcIdHash())
        , PartitionHelper() {
    }

    TActorIdentity SelfId() const {
        return TActor<TDerived>::SelfId();
    }

    [[nodiscard]] bool Initialize(const NActors::TActorContext& ctx) {
        if (TableHelper.Initialize(ctx, SourceId)) {
            return true;
        }
        StartIdle();
        TThis::ReplyError(ErrorCode::BAD_REQUEST, "Bad SourceId", ctx);
        return false;
    }

    void PassAway() {
        auto ctx = TActivationContext::ActorContextFor(SelfId());
        TableHelper.CloseKqpSession(ctx);
        PartitionHelper.Close(ctx);
        TActorBootstrapped<TDerived>::PassAway();
    }

    bool NeedTable(const NActors::TActorContext& ctx) {
        const auto& pqConfig = AppData(ctx)->PQConfig;
        return SourceId && (!pqConfig.GetTopicsAreFirstClassCitizen() || pqConfig.GetUseSrcIdMetaMappingInFirstClass());
    }

protected:
    void InitTable(const NActors::TActorContext& ctx) {
        TThis::Become(&TThis::StateInitTable);
        const auto& pqConfig = AppData(ctx)->PQConfig;
        TRACE("InitTable: SourceId="<< SourceId
              << " TopicsAreFirstClassCitizen=" << pqConfig.GetTopicsAreFirstClassCitizen()
              << " UseSrcIdMetaMappingInFirstClass=" <<pqConfig.GetUseSrcIdMetaMappingInFirstClass());
        if (SourceId && pqConfig.GetTopicsAreFirstClassCitizen() && pqConfig.GetUseSrcIdMetaMappingInFirstClass()) {
            TableHelper.SendInitTableRequest(ctx);
        } else {
            StartKqpSession(ctx);
        }
    }

    void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext& ctx) {
        StartKqpSession(ctx);
    }

    STATEFN(StateInitTable) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }

protected:
    void StartKqpSession(const NActors::TActorContext& ctx) {
        if (NeedTable(ctx)) {
            DEBUG("StartKqpSession")
            TThis::Become(&TThis::StateCreateKqpSession);
            TableHelper.SendCreateSessionRequest(ctx);
        } else {
            OnSelected(ctx);
        }
    }

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        if (!TableHelper.Handle(ev, ctx)) {
            return ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ53 : " <<  ev->Get()->Record.DebugString(), ctx);
        }

        SendSelectRequest(ctx);
    }

    void ScheduleStop() {
        TThis::Become(&TThis::StateDestroying);
    }

    STATEFN(StateCreateKqpSession) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            sFunc(TEvents::TEvPoison, ScheduleStop);
        }
    }

protected:
    void SendSelectRequest(const NActors::TActorContext& ctx) {
        TThis::Become(&TThis::StateSelect);
        DEBUG("Select from the table");
        TableHelper.SendSelectRequest(ctx);
    }

    void HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        if (!TableHelper.HandleSelect(ev, ctx)) {
            return ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ50 : " <<  ev->Get()->Record.DebugString(), ctx);
        }

        TRACE("Selected from table PartitionId=" << TableHelper.PartitionId() << " SeqNo=" << TableHelper.SeqNo());
        if (TableHelper.PartitionId()) {
            Partition = Chooser->GetPartition(TableHelper.PartitionId().value());
        }
        SeqNo = TableHelper.SeqNo();

        OnSelected(ctx);
    }

    virtual void OnSelected(const NActors::TActorContext& ctx) =  0;

    STATEFN(StateSelect) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleSelect);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }

protected:
    void SendUpdateRequests(const TActorContext& ctx) {
        if (NeedTable(ctx)) {
            TThis::Become(&TThis::StateUpdate);
            DEBUG("Update the table");
            TableHelper.SendUpdateRequest(Partition->PartitionId, SeqNo, ctx);
        } else {
            ReplyResult(ctx);
        }
    }

    void HandleUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record.GetRef();
        DEBUG("HandleUpdate PartitionPersisted=" << PartitionPersisted << " Status=" << record.GetYdbStatus());

        if (record.GetYdbStatus() == Ydb::StatusIds::ABORTED) {
            if (!PartitionPersisted) {
                TableHelper.CloseKqpSession(ctx);
                StartKqpSession(ctx);
            }
            return;
        }

        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            if (!PartitionPersisted) {
                ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ51 : " <<  record, ctx);
            }
            return;
        }

        if (!PartitionPersisted) {
            PartitionPersisted = true;
            // Use tx only for query after select. Updating AccessTime without transaction.
            TableHelper.CloseKqpSession(ctx);

            ReplyResult(ctx);
        }

        StartIdle();
    }

    STATEFN(StateUpdate) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleUpdate);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }

protected:
    void StartCheckPartitionRequest(const TActorContext &ctx) {
        TThis::Become(&TThis::StateCheckPartition);

        if (!Partition) {
            return ReplyError(TThis::PreferedPartition ? ErrorCode::WRITE_ERROR_PARTITION_INACTIVE : ErrorCode::INITIALIZING, "Partition not choosed", ctx);
        }

        PartitionHelper.Open(Partition->TabletId, ctx);
        PartitionHelper.SendCheckPartitionStatusRequest(Partition->PartitionId, "", ctx);
    }

    void Handle(NKikimr::TEvPQ::TEvCheckPartitionStatusResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        PartitionHelper.Close(ctx);
        if (NKikimrPQ::ETopicPartitionStatus::Active == ev->Get()->Record.GetStatus()) {
            return SendUpdateRequests(ctx);
        }
        ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "Partition isn`t active", ctx);
    }

    STATEFN(StateCheckPartition) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKikimr::TEvPQ::TEvCheckPartitionStatusResponse, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, HandleOwnership);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleOwnership);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }

protected:
    void HandleOwnership(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx) {
        auto msg = ev->Get();
        if (PartitionHelper.IsPipe(ev->Sender) && msg->Status != NKikimrProto::OK) {
            TableHelper.CloseKqpSession(ctx);
            ReplyError(ErrorCode::INITIALIZING, "Pipe closed", ctx);
        }
    }

    void HandleOwnership(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx) {
        if (PartitionHelper.IsPipe(ev->Sender)) {
            TableHelper.CloseKqpSession(ctx);
            ReplyError(ErrorCode::INITIALIZING, "Pipe closed", ctx);
        }
    }

protected:
    void StartIdle() {
        TThis::Become(&TThis::StateIdle);
        DEBUG("Start idle");
    }

    void HandleIdle(TEvPartitionChooser::TEvRefreshRequest::TPtr&, const TActorContext& ctx) {
        if (PartitionPersisted) {
            SendUpdateRequests(ctx);
        }
    }

    STATEFN(StateIdle)  {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPartitionChooser::TEvRefreshRequest, HandleIdle);
            SFunc(TEvents::TEvPoison, TThis::Die);
        }
    }


protected:
    void HandleDestroy(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
        TableHelper.Handle(ev, ctx);
        TThis::Die(ctx);
    }

    STATEFN(StateDestroying) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, HandleDestroy);
        }
    }

protected:
    void ReplyResult(const NActors::TActorContext& ctx) {
        if (ResultWasSent) {
            return;
        }
        ResultWasSent = true;
        DEBUG("ReplyResult: Partition=" << Partition->PartitionId << ", SeqNo=" << SeqNo);
        ctx.Send(Parent, new TEvPartitionChooser::TEvChooseResult(Partition->PartitionId, Partition->TabletId, SeqNo));
    }

    void ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx) {
        INFO("ReplyError: " << errorMessage);
        ctx.Send(Parent, new TEvPartitionChooser::TEvChooseError(code, std::move(errorMessage)));

        TThis::Die(ctx);
    }


protected:
    const TActorId Parent;
    const TString SourceId;
    const std::optional<ui32> PreferedPartition;
    const std::shared_ptr<IPartitionChooser> Chooser;

    const TPartitionInfo* Partition = nullptr;

    TTableHelper TableHelper;
    TPartitionHelper<TPipeCreator> PartitionHelper;

    bool PartitionPersisted = false;
    bool ResultWasSent = false;

    std::optional<ui64> SeqNo = 0;
};

#undef LOG_PREFIX
#undef TRACE
#undef DEBUG
#undef INFO
#undef ERROR

} // namespace NKikimr::NPQ::NPartitionChooser
