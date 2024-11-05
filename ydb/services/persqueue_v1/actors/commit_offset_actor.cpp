#include "commit_offset_actor.h"

#include "persqueue_utils.h"
#include "read_init_auth_actor.h"

#include <ydb/core/client/server/msgbus_server_persqueue.h>

#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace PersQueue::V1;


TCommitOffsetActor::TCommitOffsetActor(
        TEvCommitOffsetRequest* request, const NPersQueue::TTopicsListController& topicsHandler,
        const TActorId& schemeCache, const TActorId& newSchemeCache,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters
)
    : TBase(request)
    , SchemeCache(schemeCache)
    , NewSchemeCache(newSchemeCache)
    , AuthInitActor()
    , Counters(counters)
    , TopicsHandler(topicsHandler)
{
    Y_ASSERT(request);
}



TCommitOffsetActor::~TCommitOffsetActor() = default;


void TCommitOffsetActor::Bootstrap(const TActorContext& ctx) {
    TBase::Bootstrap(ctx);
    Become(&TThis::StateFunc);

    auto request = dynamic_cast<const Ydb::Topic::CommitOffsetRequest*>(GetProtoRequest());
    Y_ABORT_UNLESS(request);
    ClientId = NPersQueue::ConvertNewConsumerName(request->consumer(), ctx);
    PartitionId = request->Getpartition_id();

    TIntrusivePtr<NACLib::TUserToken> token;
    if (Request_->GetSerializedToken().empty()) {
        if (AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
            AnswerError("Unauthenticated access is forbidden, please provide credentials", PersQueue::ErrorCode::ACCESS_DENIED, ctx);
            return;
        }
    } else {
        token = new NACLib::TUserToken(Request_->GetSerializedToken());
    }

    THashSet<TString> topicsToResolve;

    if (request->path().empty()) {
        AnswerError("empty topic in commit offset request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
        return;
    }
    topicsToResolve.insert(request->path());

    auto topicsList = TopicsHandler.GetReadTopicsList(
            topicsToResolve, true, Request().GetDatabaseName().GetOrElse(TString())
    );
    if (!topicsList.IsValid) {
        return AnswerError(
                topicsList.Reason,
                PersQueue::ErrorCode::BAD_REQUEST, ctx
        );
    }

    AuthInitActor = ctx.Register(new TReadInitAndAuthActor(
            ctx, ctx.SelfID, ClientId, 0, TString("read_info:") + Request().GetPeerName(),
            SchemeCache, NewSchemeCache, Counters, token, topicsList, TopicsHandler.GetLocalCluster()
    ));
}


void TCommitOffsetActor::Die(const TActorContext& ctx) {
    if (PipeClient)
        NTabletPipe::CloseClient(ctx, PipeClient);

    ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());

    TActorBootstrapped<TCommitOffsetActor>::Die(ctx);
}

void TCommitOffsetActor::Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "CommitOffset auth ok, got " << ev->Get()->TopicAndTablets.size() << " topics");
    TopicAndTablets = std::move(ev->Get()->TopicAndTablets);
    if (TopicAndTablets.empty()) {
        AnswerError("empty list of topics", PersQueue::ErrorCode::UNKNOWN_TOPIC, ctx);
        return;
    }
    Y_ABORT_UNLESS(TopicAndTablets.size() == 1);
    auto& [topic, topicInitInfo] = *TopicAndTablets.begin();

    if (topicInitInfo.Partitions.find(PartitionId) == topicInitInfo.Partitions.end()) {
        AnswerError("partition id not found in topic", PersQueue::ErrorCode::WRONG_PARTITION_NUMBER, ctx);
        return;
    }

    ui64 tabletId = topicInitInfo.Partitions.at(PartitionId).TabletId;

    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };

    PipeClient = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));

    auto client_req = dynamic_cast<const Ydb::Topic::CommitOffsetRequest*>(GetProtoRequest());

    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(topicInitInfo.TopicNameConverter->GetPrimaryPath());
    request.MutablePartitionRequest()->SetPartition(client_req->partition_id());

    Y_ABORT_UNLESS(PipeClient);

    auto commit = request.MutablePartitionRequest()->MutableCmdSetClientOffset();
    commit->SetClientId(ClientId);
    commit->SetOffset(client_req->offset());
    commit->SetStrict(true);

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "strict CommitOffset, partition " << client_req->partition_id()
                        << " committing to position " << client_req->offset() /*<< " prev " << CommittedOffset
                        << " end " << EndOffset << " by cookie " << readId*/);

    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, req.Release());
}


void TCommitOffsetActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        auto errorCode = ConvertOldCode(ev->Get()->Record.GetErrorCode());
        return AnswerError(ev->Get()->Record.GetErrorReason(), errorCode, ctx);
    }

    // Convert to correct response.

    const auto& partitionResult = ev->Get()->Record.GetPartitionResponse();
    Y_ABORT_UNLESS(!partitionResult.HasCmdReadResult());

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "CommitOffset, commit done.");

    Ydb::Topic::CommitOffsetResult result;
    Request().SendResult(result, Ydb::StatusIds::SUCCESS);
    Die(ctx);
}


void TCommitOffsetActor::AnswerError(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx) {

    Ydb::Topic::CommitOffsetResponse response;
    response.mutable_operation()->set_ready(true);
    auto issue = response.mutable_operation()->add_issues();
    FillIssue(issue, errorCode, errorReason);
    response.mutable_operation()->set_status(ConvertPersQueueInternalCodeToStatus(errorCode));
    Reply(ConvertPersQueueInternalCodeToStatus(errorCode), response.operation().issues(), ctx);
}


void TCommitOffsetActor::Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    AnswerError(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}

void TCommitOffsetActor::Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
    TEvTabletPipe::TEvClientConnected *msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        AnswerError(TStringBuilder() <<"pipe to tablet is dead" << msg->TabletId, PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED, ctx);
        return;
    }
}

void TCommitOffsetActor::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
    AnswerError(TStringBuilder() <<"pipe to tablet destroyed" << ev->Get()->TabletId, PersQueue::ErrorCode::TABLET_PIPE_DISCONNECTED, ctx);
}


}
