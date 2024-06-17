#include "kqp_finalize_script_service.h"
#include "kqp_finalize_script_actor.h"

#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>

#include <ydb/library/yql/providers/s3/proto/sink.pb.h>

#include <ydb/core/protos/config.pb.h>

namespace NKikimr::NKqp {

namespace {

class TKqpFinalizeScriptService : public TActorBootstrapped<TKqpFinalizeScriptService> {
public:
    TKqpFinalizeScriptService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
        IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory)
        : QueryServiceConfig(queryServiceConfig)
        , FederatedQuerySetupFactory(federatedQuerySetupFactory)
        , S3ActorsFactory(std::move(s3ActorsFactory))
    {}

    void Bootstrap(const TActorContext &ctx) {
        FederatedQuerySetup = FederatedQuerySetupFactory->Make(ctx.ActorSystem());
        
        Become(&TKqpFinalizeScriptService::MainState);
    }

    void Handle(TEvSaveScriptExternalEffectRequest::TPtr& ev) {
        auto& description = ev->Get()->Description;
        description.Sinks = FilterExternalSinks(description.Sinks);

        if (!description.Sinks.empty()) {
            Register(CreateSaveScriptExternalEffectActor(std::move(ev)));
        } else {
            Send(ev->Sender, new TEvSaveScriptExternalEffectResponse(Ydb::StatusIds::SUCCESS, {}));
        }
    }

    void Handle(TEvScriptFinalizeRequest::TPtr& ev) {
        TString executionId = ev->Get()->Description.ExecutionId;

        if (!FinalizationRequestsQueue.contains(executionId)) {
            WaitingFinalizationExecutions.push(executionId);
        }
        FinalizationRequestsQueue[executionId].emplace_back(std::move(ev));

        TryStartFinalizeRequest();
    }

    STATEFN(MainState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvSaveScriptExternalEffectRequest, Handle);
            hFunc(TEvScriptFinalizeRequest, Handle);
            hFunc(TEvScriptFinalizeResponse, Handle);
        default:
            Y_ABORT("TKqpScriptFinalizeService: unexpected event type: %" PRIx32 " event: %s", ev->GetTypeRewrite(), ev->ToString().data());
        }
    }

private:
    void TryStartFinalizeRequest() {
        if (FinalizationRequestsInFlight >= QueryServiceConfig.GetFinalizeScriptServiceConfig().GetMaxInFlightFinalizationsCount() || WaitingFinalizationExecutions.empty()) {
            return;
        }

        TString executionId = WaitingFinalizationExecutions.front();
        WaitingFinalizationExecutions.pop();

        auto& queue = FinalizationRequestsQueue[executionId];
        Y_ENSURE(!queue.empty());

        StartFinalizeRequest(std::move(queue.back()));
        queue.pop_back();
    }

    void StartFinalizeRequest(TEvScriptFinalizeRequest::TPtr request) {
        ++FinalizationRequestsInFlight;

        Register(CreateScriptFinalizerActor(
            std::move(request),
            QueryServiceConfig,
            FederatedQuerySetup,
            S3ActorsFactory
        ));
    }

    void Handle(TEvScriptFinalizeResponse::TPtr& ev) {
        --FinalizationRequestsInFlight;
        TString executionId = ev->Get()->ExecutionId;

        if (!FinalizationRequestsQueue[executionId].empty()) {
            WaitingFinalizationExecutions.push(executionId);
        } else {
            FinalizationRequestsQueue.erase(executionId);
        }
        TryStartFinalizeRequest();
    }

private:
    bool ValidateExternalSink(const NKqpProto::TKqpExternalSink& sink) {
        if (sink.GetType() != "S3Sink") {
            return false;
        }

        NYql::NS3::TSink sinkSettings;
        sink.GetSettings().UnpackTo(&sinkSettings);

        return sinkSettings.GetAtomicUploadCommit();
    }

    std::vector<NKqpProto::TKqpExternalSink> FilterExternalSinks(const std::vector<NKqpProto::TKqpExternalSink>& sinks) {
        std::vector<NKqpProto::TKqpExternalSink> filteredSinks;
        filteredSinks.reserve(sinks.size());
        for (const auto& sink : sinks) {
            if (ValidateExternalSink(sink)) {
                filteredSinks.push_back(sink);
            }
        }

        return filteredSinks;
    }

private:
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;

    IKqpFederatedQuerySetupFactory::TPtr FederatedQuerySetupFactory;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;

    ui32 FinalizationRequestsInFlight = 0;
    std::queue<TString> WaitingFinalizationExecutions;
    std::unordered_map<TString, std::vector<TEvScriptFinalizeRequest::TPtr>> FinalizationRequestsQueue;

    std::shared_ptr<NYql::NDq::IS3ActorsFactory> S3ActorsFactory;
};

}  // anonymous namespace

IActor* CreateKqpFinalizeScriptService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory) {
    return new TKqpFinalizeScriptService(queryServiceConfig, std::move(federatedQuerySetupFactory), std::move(s3ActorsFactory));
}

}  // namespace NKikimr::NKqp
