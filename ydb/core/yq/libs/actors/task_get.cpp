#include <ydb/core/yq/libs/config/protos/yq_config.pb.h>
#include "proxy_private.h"
#include "proxy.h"

#include <ydb/core/protos/services.pb.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <library/cpp/protobuf/interop/cast.h> 

#include <ydb/core/yq/libs/common/entity_id.h>

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <ydb/library/security/util.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivateGetTask - Owner: " << OwnerId << ", " << "Host: " << Host << ", "<< stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, "PrivateGetTask - Owner: " << OwnerId << ", " << "Host: " << Host << ", " << stream)

namespace NYq {

using namespace NActors;
using namespace NMonitoring;

class TGetTaskRequestActor
    : public NActors::TActorBootstrapped<TGetTaskRequestActor>
{
public:
    TGetTaskRequestActor(
        const NActors::TActorId& sender,
        const NConfig::TTokenAccessorConfig& tokenAccessorConfig,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TAutoPtr<TEvents::TEvGetTaskRequest> ev,
        TDynamicCounterPtr counters)
        : TokenAccessorConfig(tokenAccessorConfig)
        , Sender(sender)
        , TimeProvider(timeProvider)
        , Ev(std::move(ev))
        , Counters(std::move(counters->GetSubgroup("subsystem", "private_api")->GetSubgroup("subcomponent", "GetTask")))
        , LifetimeDuration(Counters->GetHistogram("LifetimeDurationMs",  ExponentialHistogram(10, 2, 50)))
        , RequestedMBytes(Counters->GetHistogram("RequestedMB",  ExponentialHistogram(6, 2, 3)))
        , StartTime(TInstant::Now())
    {
        if (TokenAccessorConfig.GetHmacSecretFile()) {
            Signer = ::NYq::CreateSignerFromFile(TokenAccessorConfig.GetHmacSecretFile());
        }
    }

    static constexpr char ActorName[] = "YQ_PRIVATE_GET_TASK";

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr& ev, const NActors::TActorContext& ctx) {
        LOG_E("TGetTaskRequestActor::OnUndelivered");
        auto Res = MakeHolder<TEvents::TEvGetTaskResponse>();
        Res->Status = Ydb::StatusIds::GENERIC_ERROR;
        Res->Issues.AddIssue("UNDELIVERED");
        ctx.Send(ev->Sender, Res.Release());
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
        auto Res = MakeHolder<TEvents::TEvGetTaskResponse>();
        Res->Status = reqStatus;
        Res->Issues.AddIssues(Issues);
        Send(Sender, Res.Release());
        PassAway();
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TGetTaskRequestActor::StateFunc);
        const auto& req = Ev->Record;
        OwnerId = req.owner_id();
        Host = req.host();
        LOG_D("Request CP::GetTask with size: " << req.ByteSize() << " bytes");
        RequestedMBytes->Collect(req.ByteSize() / 1024 / 1024);
        ctx.Send(NYq::ControlPlaneStorageServiceActorId(),
            new NYq::TEvControlPlaneStorage::TEvGetTaskRequest(OwnerId, Host));
    }

    static TString GetServiceAccountId(const YandexQuery::IamAuth& auth) {
        return auth.has_service_account()
                ? auth.service_account().id()
                : TString{};
    }

    static TString ExtractServiceAccountId(const YandexQuery::Connection& c) {
        switch (c.content().setting().connection_case()) {
        case YandexQuery::ConnectionSetting::kYdbDatabase: {
            return GetServiceAccountId(c.content().setting().ydb_database().auth());
        }
        case YandexQuery::ConnectionSetting::kDataStreams: {
            return GetServiceAccountId(c.content().setting().data_streams().auth());
        }
        case YandexQuery::ConnectionSetting::kObjectStorage: {
            return GetServiceAccountId(c.content().setting().object_storage().auth());
        }
        case YandexQuery::ConnectionSetting::kMonitoring: {
            return GetServiceAccountId(c.content().setting().monitoring().auth());
        }
        case YandexQuery::ConnectionSetting::kClickhouseCluster: {
            return GetServiceAccountId(c.content().setting().clickhouse_cluster().auth());
        }
        // Do not replace with default. Adding a new connection should cause a compilation error
        case YandexQuery::ConnectionSetting::CONNECTION_NOT_SET:
        break;
        }
        return {};
    }

private:
    void HandleResponse(NYq::TEvControlPlaneStorage::TEvGetTaskResponse::TPtr& ev, const TActorContext& ctx) { // YQ
        LOG_D("Got CP::GetTask Response");
        const auto& tasks = ev->Get()->Tasks;
        Res->Record.ConstructInPlace();
        Res->Status = Ydb::StatusIds::SUCCESS;
        const auto& issues = ev->Get()->Issues;
        if (issues) {
            Issues.AddIssues(issues);
            Fail("ControlPlane::GetTaskError", Ydb::StatusIds::GENERIC_ERROR);
            return;
        }

        try {
            for (const auto& task : tasks) {
                const auto& queryType = task.Query.content().type();
                if (queryType != YandexQuery::QueryContent::ANALYTICS && queryType != YandexQuery::QueryContent::STREAMING) { //TODO: fix
                    ythrow yexception()
                        << "query type "
                        << YandexQuery::QueryContent::QueryType_Name(queryType)
                        << " unsupported";
                }
                auto* newTask = Res->Record->add_tasks();
                newTask->set_query_type(queryType);
                newTask->set_execute_mode(task.Query.meta().execute_mode());
                newTask->set_state_load_mode(task.Internal.state_load_mode());
                auto* queryId = newTask->mutable_query_id();
                queryId->set_value(task.Query.meta().common().id());
                newTask->set_streaming(queryType == YandexQuery::QueryContent::STREAMING);
                newTask->set_text(task.Query.content().text());
                *newTask->mutable_connection() = task.Internal.connection();
                *newTask->mutable_binding() = task.Internal.binding();
                newTask->set_user_token(task.Internal.token());
                newTask->set_user_id(task.Query.meta().common().created_by());
                newTask->set_generation(task.Generation);
                newTask->set_status(task.Query.meta().status());
                *newTask->mutable_created_topic_consumers() = task.Internal.created_topic_consumers();
                newTask->mutable_sensor_labels()->insert({"cloud_id", task.Internal.cloud_id()});
                newTask->set_automatic(task.Query.content().automatic());
                newTask->set_query_name(task.Query.content().name());
                *newTask->mutable_deadline() = NProtoInterop::CastToProto(task.Deadline);
                newTask->mutable_disposition()->CopyFrom(task.Internal.disposition());

                THashMap<TString, TString> accountIdSignatures;
                for (const auto& connection: task.Internal.connection()) {
                    const auto serviceAccountId = ExtractServiceAccountId(connection);
                    if (!serviceAccountId) {
                        continue;
                    }

                    auto& signature = accountIdSignatures[serviceAccountId];
                    if (!signature && Signer) {
                        signature = Signer->SignAccountId(serviceAccountId);
                    }
                    auto* account = newTask->add_service_accounts();
                    account->set_value(serviceAccountId);
                    account->set_signature(signature);
                }

                *newTask->mutable_dq_graph() = task.Internal.dq_graph();
                newTask->set_dq_graph_index(task.Internal.dq_graph_index());

                *newTask->mutable_result_set_meta() = task.Query.result_set_meta();
                newTask->set_scope(task.Scope);
            }
            ctx.Send(Sender, Res.Release());
            Die(ctx);
        } catch (...) {
            const auto msg = TStringBuilder() << "Can't do GetTask: " << CurrentExceptionMessage();
            Fail(msg);
        }
    }

private:
    STRICT_STFUNC(
        StateFunc,
        CFunc(NActors::TEvents::TEvPoison::EventType, Die)
        HFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
        HFunc(NYq::TEvControlPlaneStorage::TEvGetTaskResponse, HandleResponse)
    )

    const NConfig::TTokenAccessorConfig TokenAccessorConfig;
    const TActorId Sender;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TAutoPtr<TEvents::TEvGetTaskRequest> Ev;
    TDynamicCounterPtr Counters;
    const THistogramPtr LifetimeDuration;
    const THistogramPtr RequestedMBytes;
    const TInstant StartTime;

    ::NYq::TSigner::TPtr Signer;

    NYql::TIssues Issues;
    TString OwnerId;
    TString Host;

    THolder<TEvents::TEvGetTaskResponse> Res = MakeHolder<TEvents::TEvGetTaskResponse>();
};

IActor* CreateGetTaskRequestActor(
    const NActors::TActorId& sender,
    const NConfig::TTokenAccessorConfig& tokenAccessorConfig,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TAutoPtr<TEvents::TEvGetTaskRequest> ev,
    TDynamicCounterPtr counters) {
    return new TGetTaskRequestActor(
        sender,
        tokenAccessorConfig,
        timeProvider,
        std::move(ev),
        counters);
}

} /* NYq */
