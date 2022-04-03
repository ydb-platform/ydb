#include "proxy_private.h"
#include <util/datetime/base.h>

#include <ydb/core/protos/services.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h>

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <google/protobuf/util/time_util.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivatePingTask - QueryId: " << OperationId  << ", Owner: " << OwnerId  << ", " << stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivatePingTask - QueryId: " << OperationId  << ", Owner: " << OwnerId  << ", "<< stream)

namespace NYq {

using namespace NActors;
using namespace NMonitoring;

class TTaskPingRequestActor
    : public NActors::TActorBootstrapped<TTaskPingRequestActor>
{
public:
    TTaskPingRequestActor(
        const NActors::TActorId& sender,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TAutoPtr<TEvents::TEvPingTaskRequest> ev,
        TDynamicCounterPtr counters)
        : Sender(sender)
        , TimeProvider(timeProvider)
        , Ev(std::move(ev))
        , Counters(std::move(counters->GetSubgroup("subsystem", "private_api")->GetSubgroup("subcomponent", "PingTask")))
        , LifetimeDuration(Counters->GetHistogram("LifetimeDurationMs",  ExponentialHistogram(10, 2, 50)))
        , RequestedMBytes(Counters->GetHistogram("RequestedMB",  ExponentialHistogram(6, 2, 3)))
        , StartTime(TInstant::Now())
    {}

    static constexpr char ActorName[] = "YQ_PRIVATE_PING_TASK";

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev, const NActors::TActorContext& ctx) {
        LOG_E("TTaskPingRequestActor::OnUndelivered");
        auto res = MakeHolder<TEvents::TEvPingTaskResponse>();
        res->Status = Ydb::StatusIds::GENERIC_ERROR;
        res->Issues.AddIssue("UNDELIVERED");
        ctx.Send(ev->Sender, res.Release());
        Die(ctx);
    }

    void PassAway() final {
        LifetimeDuration->Collect((TInstant::Now() - StartTime).MilliSeconds());
        NActors::IActor::PassAway();
    }

    void Fail(const TString& message, Ydb::StatusIds::StatusCode reqStatus = Ydb::StatusIds::INTERNAL_ERROR) {
        Issues.AddIssue(message);
        const auto codeStr = Ydb::StatusIds_StatusCode_Name(reqStatus);
        LOG_E(TStringBuilder()
            << "Failed with code: " << codeStr
            << " Details: " << Issues.ToString());
        auto res = MakeHolder<TEvents::TEvPingTaskResponse>();
        res->Status = reqStatus;
        res->Issues.AddIssues(Issues);
        Send(Sender, res.Release());
        PassAway();
    }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Become(&TTaskPingRequestActor::StateFunc);
        const auto& req = Ev->Record;
        OperationId = req.query_id().value();
        OwnerId = req.owner_id();
        TenantName = req.tenant();
        Scope = req.scope();
        Deadline = NProtoInterop::CastFromProto(req.deadline());
        LOG_D("Request CP::PingTask with size: " << req.ByteSize() << " bytes");
        RequestedMBytes->Collect(req.ByteSize() / 1024 / 1024);
        try {
            auto event = CreateControlPlaneEvent();
            Send(NYq::ControlPlaneStorageServiceActorId(), event.release());
        } catch (const std::exception& err) {
            const auto msg = TStringBuilder() << "PingTask Boostrap Error: " << CurrentExceptionMessage();
            Fail(msg);
        }
    }

private:
    STRICT_STFUNC(
        StateFunc,
        CFunc(NActors::TEvents::TEvPoison::EventType, Die)
        HFunc(NYq::TEvControlPlaneStorage::TEvPingTaskResponse, HandleResponse)
        HFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
    )

    std::unique_ptr<NYq::TEvControlPlaneStorage::TEvPingTaskRequest> CreateControlPlaneEvent() {
        auto event = std::make_unique<NYq::TEvControlPlaneStorage::TEvPingTaskRequest>(TenantName, CloudId, Scope, OperationId, OwnerId, Deadline);
        const auto& req = Ev->Record;
        ui64 issuesByteSize = 0;
        ui64 transientIssuesByteSize = 0;
        ui64 resultSetMetaByteSize = 0;
        ui64 dqGraphBytesSize = 0;

        //TODO use all fields
        if (req.status() != YandexQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED) {
            event->Status = req.status();
        }
        if (!req.issues().empty()) {
            NYql::TIssues reqIssues;
            for (const auto& issue : req.issues()) {
                issuesByteSize += issue.ByteSize();
            }
            NYql::IssuesFromMessage(req.issues(), reqIssues);
            Issues.AddIssues(reqIssues);
            event->Issues = Issues;
        }
        if (!req.transient_issues().empty()) {
            NYql::TIssues transientIssues;
            for (const auto& issue : req.transient_issues()) {
                transientIssuesByteSize += issue.ByteSize();
            }
            NYql::IssuesFromMessage(req.transient_issues(), transientIssues);
            event->TransientIssues = transientIssues;
        }
        if (req.statistics()) {
            event->Statistics = req.statistics();
        }
        if (req.ast()) {
            event->Ast = req.ast();
        }
        if (req.result_id().value()) {
            event->ResultId = req.result_id().value();
        }
        if (req.plan()) {
            event->Plan = req.plan();
        }
        if (!req.result_set_meta().empty()) {
            for (const auto& rsMeta : req.result_set_meta()) {
                resultSetMetaByteSize += rsMeta.ByteSize();
            }
            event->ResultSetMetas = {req.result_set_meta().begin(), req.result_set_meta().end()};
        }
        if (req.has_started_at()) {
            event->StartedAt = TInstant::FromValue(google::protobuf::util::TimeUtil::TimestampToMicroseconds(req.started_at()));
        }
        if (req.has_finished_at()) {
            event->FinishedAt = TInstant::FromValue(google::protobuf::util::TimeUtil::TimestampToMicroseconds(req.finished_at()));
        }
        event->ResignQuery = req.resign_query();

        event->CreatedTopicConsumers.reserve(req.created_topic_consumers_size());
        for (const auto& topicConsumerProto : req.created_topic_consumers()) {
            auto& topicConsumer = event->CreatedTopicConsumers.emplace_back();
            topicConsumer.DatabaseId = topicConsumerProto.database_id();
            topicConsumer.Database = topicConsumerProto.database();
            topicConsumer.TopicPath = topicConsumerProto.topic_path();
            topicConsumer.ConsumerName = topicConsumerProto.consumer_name();
            topicConsumer.ClusterEndpoint = topicConsumerProto.cluster_endpoint();
            topicConsumer.UseSsl = topicConsumerProto.use_ssl();
            topicConsumer.TokenName = topicConsumerProto.token_name();
            topicConsumer.AddBearerToToken = topicConsumerProto.add_bearer_to_token();
        }

        event->DqGraphs.reserve(req.dq_graph_size());
        for (const auto& g : req.dq_graph()) {
            dqGraphBytesSize += g.size();
            event->DqGraphs.emplace_back(g);
        }

        if (req.state_load_mode()) {
            event->StateLoadMode = req.state_load_mode();
        }

        if (req.has_disposition()) {
            event->StreamingDisposition = req.disposition();
        }


        LOG_D("Statistics length: " << req.statistics().size() << ", "
           << "Ast length: " << req.ast().size() << " bytes, "
           << "Plan length: " << req.plan().size() << " bytes, "
           << "Result set meta size: " << resultSetMetaByteSize << " bytes, "
           << "Topic consumers size: " << event->CreatedTopicConsumers.size() * sizeof(TEvControlPlaneStorage::TTopicConsumer) << " bytes, "
           << "Dq graphs size: " << dqGraphBytesSize << " bytes, "
           << "Issues size: " << issuesByteSize << " bytes, "
           << "Transient issues size: " << transientIssuesByteSize << " bytes");

        event->DqGraphIndex = req.dq_graph_index();

        return std::move(event);
    }

    void HandleResponse(NYq::TEvControlPlaneStorage::TEvPingTaskResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_D("Got CP::PingTaskResponse");
        const auto& issues = ev->Get()->Issues;
        if (issues) {
            Issues.AddIssues(issues);
            Fail("ControlPlane PingTaskError", Ydb::StatusIds::GENERIC_ERROR);
            return;
        }
        auto res = MakeHolder<TEvents::TEvPingTaskResponse>();
        res->Status = Ydb::StatusIds::SUCCESS;
        Yq::Private::PingTaskResult result;
        result.set_action(ev->Get()->Action);
        res->Record.ConstructInPlace(result);
        ctx.Send(Sender, res.Release());
        Die(ctx);
    }

private:
    const TActorId Sender;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TAutoPtr<TEvents::TEvPingTaskRequest> Ev;
    TDynamicCounterPtr Counters;
    const THistogramPtr LifetimeDuration;
    const THistogramPtr RequestedMBytes;
    const TInstant StartTime;

    TString OperationId;
    TString OwnerId;
    TString TenantName;
    TString CloudId;
    TString Scope;
    TInstant Deadline;
    NYql::TIssues Issues;
};

IActor* CreatePingTaskRequestActor(
    const NActors::TActorId& sender,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvPingTaskRequest> ev,
    TDynamicCounterPtr counters) {
    return new TTaskPingRequestActor(
        sender,
        timeProvider,
        std::move(ev),
        std::move(counters));
}

} /* NYq */
