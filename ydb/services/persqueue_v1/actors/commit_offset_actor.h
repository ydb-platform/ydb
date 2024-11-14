#pragma once

#include "events.h"
#include "ydb/core/kqp/common/simple/services.h"
#include "ydb/services/metadata/service.h"


#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>

#include <ydb/core/persqueue/events/global.h>


namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;

class TKqpHelper {

    public: // savnik

    TString DataBase;
    TString KqpSessionId;
    TString Consumer;
    TString Path;
    TString TxId;

    int Step = 0;

    struct TCommitInfo {
        ui64 PartitionId;
        i64 Offset;
    };

    void SendCreateSessionRequest(const TActorContext& ctx) {
        auto ev = MakeCreateSessionRequest();
        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
    }

    THolder<NKqp::TEvKqp::TEvCreateSessionRequest> MakeCreateSessionRequest() {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCreateSessionRequest>();
        ev->Record.MutableRequest()->SetDatabase(DataBase);
        return ev;
    }

    bool Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const TActorContext& /*ctx*/)  {
        const auto& record = ev->Get()->Record;

        if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return false;
        }

        KqpSessionId = record.GetResponse().GetSessionId();
        Y_ABORT_UNLESS(!KqpSessionId.empty());

        return true;
    }

    void CloseKqpSession(const TActorContext& ctx) {
        if (KqpSessionId) {
            auto ev = MakeCloseSessionRequest();
            ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

            KqpSessionId = "";
        }
    }

    THolder<NKqp::TEvKqp::TEvCloseSessionRequest> MakeCloseSessionRequest() {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(KqpSessionId);
        return ev;
    }

    void SendCommits(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, std::vector<TCommitInfo> commits, const NActors::TActorContext& ctx)
    {
        // if (!AppData(ctx)->FeatureFlags.GetEnableTopicServiceTx()) { // savnik need this check?
        //     return Reply(Ydb::StatusIds::UNSUPPORTED,
        //                 "Disabled transaction support for TopicService.",
        //                 NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
        //                 ctx);
        // }

        auto& record = ev->Get()->Record;
        TxId = record.GetResponse().GetTxMeta().id();
        Y_ABORT_UNLESS(!TxId.empty());


        auto offsets = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        offsets->Record.MutableRequest()->SetDatabase(DataBase);
        offsets->Record.MutableRequest()->SetSessionId(KqpSessionId);
        offsets->Record.MutableRequest()->SetType(NKikimrKqp::QUERY_TYPE_UNDEFINED);
        offsets->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_TOPIC);
        offsets->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
        offsets->Record.MutableRequest()->MutableTopicOperations()->SetConsumer(Consumer);

        // savnik need set something else?

        auto* topic = offsets->Record.MutableRequest()->MutableTopicOperations()->AddTopics();

        topic->set_path(Path);

        for(auto &c: commits) {
            auto* partition = topic->add_partitions();
            partition->set_partition_id(c.PartitionId);
            partition->set_force_commit(true);
            partition->set_kill_read_session(true);
            auto* offset = partition->add_partition_offsets();
            offset->set_end(c.Offset);
        }

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), offsets.Release());
    }

    void BeginTransaction(const NActors::TActorContext& ctx) {
        auto begin = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        begin->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_BEGIN_TX);
        begin->Record.MutableRequest()->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
        begin->Record.MutableRequest()->SetSessionId(KqpSessionId);
        begin->Record.MutableRequest()->SetDatabase(DataBase);

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), begin.Release());
    }

    void CommitTx(const NActors::TActorContext& ctx) {
        auto commit = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();

        commit->Record.MutableRequest()->SetAction(NKikimrKqp::QUERY_ACTION_COMMIT_TX);
        commit->Record.MutableRequest()->MutableTxControl()->set_tx_id(TxId);
        commit->Record.MutableRequest()->MutableTxControl()->set_commit_tx(true);
        commit->Record.MutableRequest()->SetSessionId(KqpSessionId);
        commit->Record.MutableRequest()->SetDatabase(DataBase);

        ctx.Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), commit.Release());
    }
};

class TCommitOffsetActor : public TRpcOperationRequestActor<TCommitOffsetActor, TEvCommitOffsetRequest> {

    using TBase = TRpcOperationRequestActor<TCommitOffsetActor, TEvCommitOffsetRequest>;

    using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
    using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;

public:
    static constexpr ui32 MAX_PIPE_RESTARTS = 100; //after 100 restarts without progress kill session

public:
     TCommitOffsetActor(
             NKikimr::NGRpcService::TEvCommitOffsetRequest* request, const NPersQueue::TTopicsListController& topicsHandler,
             const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
             TIntrusivePtr<::NMonitoring::TDynamicCounters> counters
     );
    ~TCommitOffsetActor();

    void Bootstrap(const NActors::TActorContext& ctx);


    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_COMMIT; }

    bool HasCancelOperation() {
        return false;
    }

private:

    void Die(const NActors::TActorContext& ctx) override;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQProxy::TEvAuthResultOk, Handle); // from auth actor
            HFunc(TEvPQProxy::TEvCloseSession, Handle); // from auth actor

            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

            HFunc(TEvPersQueue::TEvResponse, Handle);

            HFunc(NKqp::TEvKqp::TEvCreateSessionResponse, Handle);
            HFunc(NKqp::TEvKqp::TEvQueryResponse, Handle);
        default:
            break;
        };
    }

    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);

    void Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);

    void SendCommit(const TTopicInitInfo& topicInitInfo, const Ydb::Topic::CommitOffsetRequest* commitRequest, const TActorContext& ctx);
    void SendDistributedTxOffsets(const TActorContext& ctx);

    void AnswerError(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx);
    void ProcessAnswers(const TActorContext& ctx);

private:
    TActorId SchemeCache;
    TActorId NewSchemeCache;

    TActorId AuthInitActor;

    TTopicInitInfoMap TopicAndTablets;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TString ClientId;
    ui64 PartitionId;

    TActorId PipeClient;

    NPersQueue::TTopicsListController TopicsHandler;

    TKqpHelper Kqp;
    std::vector<TKqpHelper::TCommitInfo> Commits;
};

}
