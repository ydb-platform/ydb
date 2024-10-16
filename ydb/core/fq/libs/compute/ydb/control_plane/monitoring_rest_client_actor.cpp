#include <ydb/core/fq/libs/compute/ydb/events/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <ydb/library/security/util.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/utils/actors/http_sender.h>
#include <ydb/library/yql/utils/actors/http_sender_actor.h>
#include <ydb/library/yql/utils/url_builder.h>

#include <library/cpp/json/json_reader.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringRestClient]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringRestClient]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringRestClient]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringRestClient]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [MonitoringRestClient]: " << stream)

namespace NFq {

using namespace NActors;

class TMonitoringRestServiceActor : public NActors::TActor<TMonitoringRestServiceActor> {
public:
    using TBase = NActors::TActor<TMonitoringRestServiceActor>;

    TMonitoringRestServiceActor(const TString& endpoint, const TString& database, const NYdb::TCredentialsProviderPtr& credentialsProvider)
        : TBase(&TMonitoringRestServiceActor::StateFunc)
        , Endpoint(endpoint)
        , Database(database)
        , CredentialsProvider(credentialsProvider)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCpuLoadRequest, Handle);
        hFunc(NYql::NDq::TEvHttpBase::TEvSendResult, Handle);
    )

    void Handle(TEvYdbCompute::TEvCpuLoadRequest::TPtr& ev) {
        if (Y_UNLIKELY(!HttpProxyId)) {
            HttpProxyId = Register(NHttp::CreateHttpProxy(NMonitoring::TMetricRegistry::SharedInstance()));
        }

        auto httpRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(
            NYql::TUrlBuilder(Endpoint)
                .AddPathComponent("viewer")
                .AddPathComponent("json")
                .AddPathComponent("tenantinfo")
                .AddUrlParam("path", Database)
                .Build()
        );
        auto ticket = CredentialsProvider->GetAuthInfo();
        LOG_D(httpRequest->GetObfuscatedData() << " using ticket " << NKikimr::MaskTicket(ticket));
        httpRequest->Set("Authorization", ticket);

        auto httpSenderId = Register(NYql::NDq::CreateHttpSenderActor(SelfId(), HttpProxyId, NYql::NDq::THttpSenderRetryPolicy::GetNoRetryPolicy()));
        Send(httpSenderId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest), 0, Cookie);
        Requests[Cookie++] = ev;
    }

    void Handle(NYql::NDq::TEvHttpBase::TEvSendResult::TPtr& ev) {
        auto it = Requests.find(ev->Cookie);
        if (it == Requests.end()) {
            LOG_E("Request doesn't exist (TEvSendResult). Need to fix this bug urgently");
            return;
        }
        auto request = it->second;
        Requests.erase(it);

        const auto& result = *ev->Get();
        const auto& response = *result.HttpIncomingResponse->Get();

        auto forwardResponse = std::make_unique<TEvYdbCompute::TEvCpuLoadResponse>();

        const TString& error = response.GetError();
        if (!error.empty()) {
            forwardResponse->Issues.AddIssue(error);
            Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
            return;
        }

        try {
            NJson::TJsonReaderConfig jsonConfig;
            NJson::TJsonValue info;
            if (NJson::ReadJsonTree(response.Response->Body, &jsonConfig, &info)) {
                bool usageFound = false;
                if (auto* tenantNode = info.GetValueByPath("TenantInfo")) {
                    if (tenantNode->GetType() == NJson::JSON_ARRAY) {
                        for (auto tenantItem : tenantNode->GetArray()) {
                            if (auto* nameNode = tenantItem.GetValueByPath("Name")) {
                                if (nameNode->GetStringSafe() != Database) {
                                    continue;
                                }
                            }
                            if (auto* poolNode = tenantItem.GetValueByPath("PoolStats")) {
                                if (poolNode->GetType() == NJson::JSON_ARRAY) {
                                    for (auto poolItem : poolNode->GetArray()) {
                                        if (auto* nameNode = poolItem.GetValueByPath("Name")) {
                                            if (nameNode->GetStringSafe() == "User") {
                                                if (auto* usageNode = poolItem.GetValueByPath("Usage")) {
                                                    forwardResponse->InstantLoad = usageNode->GetDoubleSafe();
                                                    usageFound = true;
                                                    break;
                                                }
                                                if (auto* threadsNode = poolItem.GetValueByPath("Threads")) {
                                                    forwardResponse->CpuNumber = threadsNode->GetIntegerSafe();
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            if (usageFound) {
                                break;
                            }
                        }
                    }
                }
                if (!usageFound) {
                    forwardResponse->Issues.AddIssue(TStringBuilder() << "MISSED User pool node load for database \"" << Database << '"');
                }
            } else {
                forwardResponse->Issues.AddIssue("Malformed JSON");
            }
        } catch(const std::exception& e) {
            forwardResponse->Issues.AddIssue(TStringBuilder() << "Error on JSON parsing: '" << e.what() << "'");
        }

        if (forwardResponse->Issues) {
            LOG_E(response.Response->Body);
        }
        Send(request->Sender, forwardResponse.release(), 0, request->Cookie);
    }

private:
    TString Endpoint;
    TString Database;
    TMap<uint64_t, TEvYdbCompute::TEvCpuLoadRequest::TPtr> Requests;
    NYdb::TCredentialsProviderPtr CredentialsProvider;
    int64_t Cookie = 0;
    TActorId HttpProxyId;
};

std::unique_ptr<NActors::IActor> CreateMonitoringRestClientActor(const TString& endpoint, const TString& database, const NYdb::TCredentialsProviderPtr& credentialsProvider) {
    return std::make_unique<TMonitoringRestServiceActor>(endpoint, database, credentialsProvider);
}

}
