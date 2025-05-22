#include "read_info_actor.h"

#include "persqueue_utils.h"
#include "read_init_auth_actor.h"

#include <ydb/core/client/server/msgbus_server_persqueue.h>

#include <ydb/public/api/protos/ydb_persqueue_v1.pb.h>
#include <ydb/public/lib/base/msgbus_status.h>

namespace NKikimr::NGRpcProxy::V1 {

using namespace PersQueue::V1;


TReadInfoActor::TReadInfoActor(
        TEvPQReadInfoRequest* request, const NPersQueue::TTopicsListController& topicsHandler,
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



TReadInfoActor::~TReadInfoActor() = default;


void TReadInfoActor::Bootstrap(const TActorContext& ctx) {
    TBase::Bootstrap(ctx);
    Become(&TThis::StateFunc);

    auto request = dynamic_cast<const ReadInfoRequest*>(GetProtoRequest());
    Y_ABORT_UNLESS(request);
    ClientId = NPersQueue::ConvertNewConsumerName(request->consumer().path(), ctx);

    bool readOnlyLocal = request->get_only_original();

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

    for (auto& t : request->topics()) {
        if (t.path().empty()) {
            AnswerError("empty topic in init request", PersQueue::ErrorCode::BAD_REQUEST, ctx);
            return;
        }
        topicsToResolve.insert(t.path());
    }
    auto topicsList = TopicsHandler.GetReadTopicsList(
            topicsToResolve, readOnlyLocal, Request().GetDatabaseName().GetOrElse(TString())
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


void TReadInfoActor::Die(const TActorContext& ctx) {

    ctx.Send(AuthInitActor, new TEvents::TEvPoisonPill());

    TActorBootstrapped<TReadInfoActor>::Die(ctx);
}


void TReadInfoActor::Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx) {

    LOG_DEBUG_S(ctx, NKikimrServices::PQ_READ_PROXY, "GetReadInfo auth ok fo read info, got " << ev->Get()->TopicAndTablets.size() << " topics");
    TopicAndTablets = std::move(ev->Get()->TopicAndTablets);
    if (TopicAndTablets.empty()) {
        AnswerError("empty list of topics", PersQueue::ErrorCode::UNKNOWN_TOPIC, ctx);
        return;
    }

    NKikimrClient::TPersQueueRequest proto;
    proto.MutableMetaRequest()->MutableCmdGetReadSessionsInfo()->SetClientId(ClientId);
    for (auto& [_, t] : TopicAndTablets) {
        proto.MutableMetaRequest()->MutableCmdGetReadSessionsInfo()->AddTopic(t.TopicNameConverter->GetClientsideName());
    }

    ctx.Register(NMsgBusProxy::CreateActorServerPersQueue(
        ctx.SelfID,
        proto,
        SchemeCache
    ));

}


void TReadInfoActor::Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
    if (ev->Get()->Record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
        // map NMsgBusProxy::EResponseStatus to PersQueue::ErrorCode???
        return AnswerError(ev->Get()->Record.GetErrorReason(), PersQueue::ErrorCode::ERROR, ctx);
    }

    // Convert to correct response.

    ReadInfoResult result;

    const auto& resp = ev->Get()->Record;
    Y_ABORT_UNLESS(resp.HasMetaResponse());

    Y_ABORT_UNLESS(resp.GetMetaResponse().GetCmdGetReadSessionsInfoResult().TopicResultSize() == TopicAndTablets.size());
    TMap<std::pair<TString, ui64>, ReadInfoResult::TopicInfo::PartitionInfo*> partResultMap;
    for (auto& tt : resp.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetTopicResult()) {
        auto topicRes = result.add_topics();
        topicRes->mutable_topic()->set_path(NPersQueue::GetTopicPath(tt.GetTopic()));
        topicRes->set_cluster(NPersQueue::GetDC(tt.GetTopic()));
        topicRes->set_status(ConvertPersQueueInternalCodeToStatus(ConvertOldCode(tt.GetErrorCode())));
        if (tt.GetErrorCode() != NPersQueue::NErrorCode::OK)
            FillIssue(topicRes->add_issues(), ConvertOldCode(tt.GetErrorCode()), tt.GetErrorReason());

        for (auto& pp : tt.GetPartitionResult()) {
            auto partRes = topicRes->add_partitions();

            partRes->set_partition(pp.GetPartition());
            partRes->set_status(ConvertPersQueueInternalCodeToStatus(ConvertOldCode(pp.GetErrorCode())));
            if (pp.GetErrorCode() != NPersQueue::NErrorCode::OK)
                FillIssue(partRes->add_issues(), ConvertOldCode(pp.GetErrorCode()), pp.GetErrorReason());

            partRes->set_start_offset(pp.GetStartOffset());
            partRes->set_end_offset(pp.GetEndOffset());

            partRes->set_commit_offset(pp.GetClientOffset());
            partRes->set_commit_time_lag_ms(pp.GetTimeLag());

            partRes->set_read_offset(pp.GetClientReadOffset());
            partRes->set_read_time_lag_ms(pp.GetReadTimeLag());

            partRes->set_session_id(pp.GetSession()); //TODO: fill error when no session returned result

            partRes->set_client_node(pp.GetClientNode());
            partRes->set_proxy_node(pp.GetProxyNode());
            partRes->set_tablet_node(pp.GetTabletNode());
            partResultMap[std::make_pair<TString, ui64>(TString(tt.GetTopic()), pp.GetPartition())] = partRes;
        }
    }
    for (auto& ss : resp.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetSessionResult()) {
        for (auto& pr : ss.GetPartitionResult()) {
            auto it = partResultMap.find(std::make_pair<TString, ui64>(TString(pr.GetTopic()), pr.GetPartition()));
            if (it == partResultMap.end())
                continue;
            auto sesRes = it->second;
            sesRes->set_session_id(ss.GetSession());
            sesRes->set_status(ConvertPersQueueInternalCodeToStatus(ConvertOldCode(ss.GetErrorCode())));
            if (ss.GetErrorCode() != NPersQueue::NErrorCode::OK) //TODO: what if this is result for already dead session?
                FillIssue(sesRes->add_issues(), ConvertOldCode(ss.GetErrorCode()), ss.GetErrorReason());

            for (auto& nc : pr.GetNextCommits()) {
                sesRes->add_out_of_order_read_cookies_to_commit(nc);
            }
            sesRes->set_last_read_cookie(pr.GetLastReadId());
            sesRes->set_committed_read_cookie(pr.GetReadIdCommitted());
            sesRes->set_assign_timestamp_ms(pr.GetTimestamp());

            sesRes->set_client_node(ss.GetClientNode());
            sesRes->set_proxy_node(ss.GetProxyNode());
        }
    }
    Request().SendResult(result, Ydb::StatusIds::SUCCESS);
    Die(ctx);
}


void TReadInfoActor::AnswerError(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx) {

    ReadInfoResponse response;
    response.mutable_operation()->set_ready(true);
    auto issue = response.mutable_operation()->add_issues();
    FillIssue(issue, errorCode, errorReason);
    response.mutable_operation()->set_status(ConvertPersQueueInternalCodeToStatus(errorCode));
    Reply(ConvertPersQueueInternalCodeToStatus(errorCode), response.operation().issues(), ctx);
}


void TReadInfoActor::Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx) {
    AnswerError(ev->Get()->Reason, ev->Get()->ErrorCode, ctx);
}

}
