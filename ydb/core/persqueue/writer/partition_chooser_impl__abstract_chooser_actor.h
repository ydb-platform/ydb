#pragma once

#include "partition_chooser.h"
#include "partition_chooser_impl__partition_helper.h"
#include "partition_chooser_impl__table_helper.h"

#include <ydb/core/persqueue/public/pq_database.h>
#include <ydb/core/persqueue/writer/metadata_initializers.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/wilson_ids/wilson.h>

namespace NKikimr::NPQ::NPartitionChooser {

#if defined(LOG_PREFIX)
#error "Already defined LOG_PREFIX"
#endif


#define LOG_PREFIX TStringBuilder() << "TPartitionChooser " << SelfId()                  \
                    << " (SourceId=" << SourceId                     \
                    << ", PreferedPartition=" << PreferedPartition   \
                    << ") "

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
                                   const std::shared_ptr<IPartitionChooser>& chooser,
                                   NPersQueue::TTopicConverterPtr& fullConverter,
                                   const TString& sourceId,
                                   std::optional<ui32> preferedPartition,
                                   NWilson::TTraceId traceId)
        : Parent(parentId)
        , SourceId(sourceId)
        , PreferedPartition(preferedPartition)
        , Chooser(chooser)
        , Span(TWilsonTopic::TopicDetailed, std::move(traceId), "Topic.ChoosePartition")
        , TableHelper(fullConverter->GetClientsideName(), fullConverter->GetTopicForSrcIdHash())
        , PartitionHelper(Span.GetTraceId())
    {
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
        YDB_LOG_TRACE_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "InitTable",
            {"logPrefix", LOG_PREFIX},
            {"sourceId", SourceId},
            {"topicsAreFirstClassCitizen", pqConfig.GetTopicsAreFirstClassCitizen()},
            {"useSrcIdMetaMappingInFirstClass", pqConfig.GetUseSrcIdMetaMappingInFirstClass()});
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
            YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "StartKqpSession",
                {"logPrefix", LOG_PREFIX});
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
        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "Select from the table",
            {"logPrefix", LOG_PREFIX});
        TableHelper.SendSelectRequest(ctx);
    }

    void HandleSelect(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        if (!TableHelper.HandleSelect(ev, ctx)) {
            return ReplyError(ErrorCode::INITIALIZING, TStringBuilder() << "kqp error Marker# PQ50 : " <<  ev->Get()->Record.DebugString(), ctx);
        }

        YDB_LOG_TRACE_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "Selected from table",
            {"logPrefix", LOG_PREFIX},
            {"partitionId", TableHelper.PartitionId()},
            {"seqNo", TableHelper.SeqNo()});
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
            YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "Update the table",
                {"logPrefix", LOG_PREFIX});
            TableHelper.SendUpdateRequest(Partition->PartitionId, SeqNo, ctx);
        } else {
            ReplyResult(ctx);
        }
    }

    void HandleUpdate(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
        auto& record = ev->Get()->Record;
        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "HandleUpdate",
            {"logPrefix", LOG_PREFIX},
            {"partitionPersisted", PartitionPersisted},
            {"status", record.GetYdbStatus()});

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
        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "Start idle",
            {"logPrefix", LOG_PREFIX});
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
        YDB_LOG_DEBUG_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "ReplyResult",
            {"logPrefix", LOG_PREFIX},
            {"partition", Partition->PartitionId},
            {"seqNo", SeqNo});
        Span.EndOk();
        Span = {};
        ctx.Send(Parent, new TEvPartitionChooser::TEvChooseResult(Partition->PartitionId, Partition->TabletId, SeqNo));
    }

    void ReplyError(ErrorCode code, TString&& errorMessage, const NActors::TActorContext& ctx) {
        YDB_LOG_INFO_COMP(NKikimrServices::PQ_PARTITION_CHOOSER, "",
            {"logPrefix", LOG_PREFIX},
            {"replyError", errorMessage});
        Span.EndError(errorMessage);
        ctx.Send(Parent, new TEvPartitionChooser::TEvChooseError(code, std::move(errorMessage)));

        TThis::Die(ctx);
    }


protected:
    const TActorId Parent;
    const TString SourceId;
    const std::optional<ui32> PreferedPartition;
    const std::shared_ptr<IPartitionChooser> Chooser;
    NWilson::TSpan Span;

    const TPartitionInfo* Partition = nullptr;

    TTableHelper TableHelper;
    TPartitionHelper<TPipeCreator> PartitionHelper;

    bool PartitionPersisted = false;
    bool ResultWasSent = false;

    std::optional<ui64> SeqNo = 0;
};

#undef LOG_PREFIX

} // namespace NKikimr::NPQ::NPartitionChooser
