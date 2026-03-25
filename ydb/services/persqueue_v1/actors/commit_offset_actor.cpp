#include "commit_offset_actor.h"

#include "persqueue_utils.h"
#include "read_init_auth_actor.h"

#include <ydb/core/client/server/msgbus_server_persqueue.h>

#include <ydb/core/persqueue/common/actor.h>
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
    , TopicsHandler(std::make_unique<NPersQueue::TTopicsListController>(topicsHandler))
{
    Y_ASSERT(request);
}

TCommitOffsetActor::TCommitOffsetActor(NKikimr::NGRpcService::IRequestOpCtx * ctx)
    : TBase(ctx)
    , SchemeCache(NMsgBusProxy::CreatePersQueueMetaCacheV2Id())
    , NewSchemeCache(MakeSchemeCacheID())
    , AuthInitActor()
    , Counters(nullptr)
{
}

TCommitOffsetActor::~TCommitOffsetActor() = default;


void TCommitOffsetActor::Bootstrap(const TActorContext& ctx) {
    TBase::Bootstrap(ctx);
    Become(&TThis::StateFunc);

    auto request = dynamic_cast<const Ydb::Topic::CommitOffsetRequest*>(GetProtoRequest());
    AFL_ENSURE(request);
    ClientId = NPersQueue::ConvertNewConsumerName(request->consumer(), ctx);
    PartitionId = request->partition_id();

    if (TopicsHandler == nullptr) {
        TopicConverterFactory = std::make_shared<NPersQueue::TTopicNamesConverterFactory>(
            NKikimrPQ::TPQConfig(), ""
        );
        TopicsHandler = std::make_unique<NPersQueue::TTopicsListController>(
                TopicConverterFactory
        );
    }

    TIntrusivePtr<NACLib::TUserToken> token;
    if (Request_->GetSerializedToken().empty()) {
        if (AppData(ctx)->EnforceUserTokenRequirement || AppData(ctx)->PQConfig.GetRequireCredentialsInNewProtocol()) {
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

    auto topicsList = TopicsHandler->GetReadTopicsList(
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
            SchemeCache, NewSchemeCache, Counters, token, topicsList, TopicsHandler->GetLocalCluster()
    ));
}

bool TCommitOffsetActor::OnUnhandledException(const std::exception& exc) {
    NPQ::DoLogUnhandledException(NKikimrServices::PQ_READ_PROXY, "[CommitOffsetActor]", exc);

    Ydb::Topic::CommitOffsetResult result;
    Request().SendResult(result, Ydb::StatusIds::INTERNAL_ERROR);

    this->Die(ActorContext());

    return true;
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
    AFL_ENSURE(TopicAndTablets.size() == 1);
    auto& [_, topicInitInfo] = *TopicAndTablets.begin();

    if (topicInitInfo.Partitions.find(PartitionId) == topicInitInfo.Partitions.end()) {
        AnswerError("partition id not found in topic", PersQueue::ErrorCode::WRONG_PARTITION_NUMBER, ctx);
        return;
    }

    auto commitRequest = dynamic_cast<const Ydb::Topic::CommitOffsetRequest*>(GetProtoRequest());

    auto* partitionNode = topicInitInfo.PartitionGraph->GetPartition(commitRequest->partition_id());

    if (partitionNode->AllParents.size() == 0 && partitionNode->DirectChildren.size() == 0) {
        SendCommit(topicInitInfo, commitRequest, ctx);
    } else {
        auto hasReadSession = !commitRequest->read_session_id().empty();
        auto killReadSession = !hasReadSession;
        const TString& readSessionId = commitRequest->read_session_id();

        std::vector<TDistributedCommitHelper::TCommitInfo> commits;

        for (auto& parent: partitionNode->AllParents) {
            TDistributedCommitHelper::TCommitInfo commit {
                .PartitionId = parent->Id,
                .Offset = Max<i64>(),
                .KillReadSession = killReadSession,
                .OnlyCheckCommitedToFinish = false,
                .ReadSessionId = readSessionId
            };
            commits.push_back(commit);
        }

        if (!hasReadSession) {
            for (auto& child: partitionNode->AllChildren) {
                TDistributedCommitHelper::TCommitInfo commit {
                    .PartitionId = child->Id,
                    .Offset = 0,
                    .KillReadSession = true,
                    .OnlyCheckCommitedToFinish = false
                };
                commits.push_back(commit);
            }
        }

        TDistributedCommitHelper::TCommitInfo commit {
            .PartitionId = partitionNode->Id,
            .Offset = commitRequest->offset(),
            .KillReadSession = killReadSession,
            .OnlyCheckCommitedToFinish = false,
            .ReadSessionId = readSessionId
        };
        commits.push_back(commit);

        auto topic = topicInitInfo.TopicNameConverter->GetPrimaryPath();
        Kqp = std::make_unique<TDistributedCommitHelper>(Request().GetDatabaseName().GetOrElse(TString()), ClientId, topic, commits);
        Kqp->SendCreateSessionRequest(ctx);
    }
}

void TCommitOffsetActor::Handle(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev, const NActors::TActorContext& ctx) {
    if (!Kqp->Handle(ev, ctx)) {
        AnswerError(ev->Get()->Record.GetError(), PersQueue::ErrorCode::ERROR, ctx);
    }
}

void TCommitOffsetActor::Handle(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "strict CommitOffset failed. Kqp error: " << ev->Get()->Record);

        Ydb::Topic::CommitOffsetResult result;
        Request().SendResult(result, record.GetYdbStatus());
        Die(ctx);
        return;
    }

    auto step = Kqp->Handle(ev, ctx);

    if (step == TDistributedCommitHelper::ECurrentStep::DONE) {
        Ydb::Topic::CommitOffsetResult result;
        Request().SendResult(result, Ydb::StatusIds::SUCCESS);
        Die(ctx);
        return;
    }
}

void TCommitOffsetActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        auto errorCode = ConvertOldCode(ev->Get()->Record.GetErrorCode());
        return AnswerError(ev->Get()->Record.GetErrorReason(), errorCode, ctx);
    }

    // Convert to correct response.

    const auto& partitionResult = ev->Get()->Record.GetPartitionResponse();
    AFL_ENSURE(!partitionResult.HasCmdReadResult());

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "CommitOffset, commit done.");

    Ydb::Topic::CommitOffsetResult result;
    Request().SendResult(result, Ydb::StatusIds::SUCCESS);
    Die(ctx);
}

void TCommitOffsetActor::SendCommit(const TTopicInitInfo& topic, const Ydb::Topic::CommitOffsetRequest* commitRequest, const TActorContext& ctx) {
    ui64 tabletId = topic.Partitions.at(PartitionId).TabletId;

    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 6,
        .MinRetryTime = TDuration::MilliSeconds(10),
        .MaxRetryTime = TDuration::MilliSeconds(100),
        .BackoffMultiplier = 2,
        .DoFirstRetryInstantly = true
    };

    PipeClient = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));

    NKikimrClient::TPersQueueRequest request;
    request.MutablePartitionRequest()->SetTopic(topic.TopicNameConverter->GetPrimaryPath());
    request.MutablePartitionRequest()->SetPartition(commitRequest->partition_id());

    AFL_ENSURE(PipeClient);

    auto commit = request.MutablePartitionRequest()->MutableCmdSetClientOffset();
    commit->SetClientId(ClientId);
    commit->SetOffset(commitRequest->offset());
    commit->SetStrict(true);
    if (!commitRequest->read_session_id().empty()) {
        commit->SetSessionId(commitRequest->read_session_id());
    }

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "strict CommitOffset, partition " << commitRequest->partition_id()
                        << " committing to position " << commitRequest->offset() /*<< " prev " << CommittedOffset
                        << " end " << EndOffset << " by cookie " << readId*/);

    TAutoPtr<TEvPersQueue::TEvRequest> req(new TEvPersQueue::TEvRequest);
    req->Record.Swap(&request);

    NTabletPipe::SendData(ctx, PipeClient, req.Release());
}

void TCommitOffsetActor::AnswerError(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx) {
    Ydb::Topic::CommitOffsetResponse response;
    response.mutable_operation()->set_ready(true);
    auto issue = response.mutable_operation()->add_issues();
    FillIssue(issue, errorCode, errorReason);
    auto status = ConvertPersQueueInternalCodeToStatus(errorCode);
    response.mutable_operation()->set_status(status);
    Reply(status, response.operation().issues(), ctx);
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
