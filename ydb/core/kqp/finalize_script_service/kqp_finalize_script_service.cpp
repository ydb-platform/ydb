#include "kqp_finalize_script_service.h"
#include "kqp_finalize_script_actor.h"

#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>

#include <ydb/library/yql/providers/s3/proto/sink.pb.h>


namespace NKikimr::NKqp {

namespace {

class TKqpFinalizeScriptService : public TActorBootstrapped<TKqpFinalizeScriptService> {
public:
    TKqpFinalizeScriptService(const NKikimrConfig::TFinalizeScriptServiceConfig& finalizeScriptServiceConfig,
        const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
        IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory)
        : FinalizeScriptServiceConfig_(finalizeScriptServiceConfig)
        , MetadataProviderConfig_(metadataProviderConfig)
        , FederatedQuerySetupFactory_(federatedQuerySetupFactory)
    {}

    void Bootstrap(const TActorContext &ctx) {
        FederatedQuerySetup_ = FederatedQuerySetupFactory_->Make(ctx.ActorSystem());
        
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

        if (!FinalizationRequestsQueue_.contains(executionId)) {
            WaitingFinalizationExecutions_.push(executionId);
        }
        FinalizationRequestsQueue_[executionId].emplace_back(std::move(ev));

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
        if (FinalizationRequestsInFlight_ >= FinalizeScriptServiceConfig_.GetMaxInFlightFinalizationsCount() || WaitingFinalizationExecutions_.empty()) {
            return;
        }

        TString executionId = WaitingFinalizationExecutions_.front();
        WaitingFinalizationExecutions_.pop();

        auto& queue = FinalizationRequestsQueue_[executionId];
        Y_ENSURE(!queue.empty());

        StartFinalizeRequest(std::move(queue.back()));
        queue.pop_back();
    }

    void StartFinalizeRequest(TEvScriptFinalizeRequest::TPtr request) {
        ++FinalizationRequestsInFlight_;

        Register(CreateScriptFinalizerActor(
            std::move(request),
            FinalizeScriptServiceConfig_,
            MetadataProviderConfig_,
            FederatedQuerySetup_
        ));
    }

    void Handle(TEvScriptFinalizeResponse::TPtr& ev) {
        --FinalizationRequestsInFlight_;
        TString executionId = ev->Get()->ExecutionId;

        if (!FinalizationRequestsQueue_[executionId].empty()) {
            WaitingFinalizationExecutions_.push(executionId);
        } else {
            FinalizationRequestsQueue_.erase(executionId);
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
    NKikimrConfig::TFinalizeScriptServiceConfig FinalizeScriptServiceConfig_;
    NKikimrConfig::TMetadataProviderConfig MetadataProviderConfig_;

    IKqpFederatedQuerySetupFactory::TPtr FederatedQuerySetupFactory_;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup_;

    ui32 FinalizationRequestsInFlight_ = 0;
    std::queue<TString> WaitingFinalizationExecutions_;
    std::unordered_map<TString, std::vector<TEvScriptFinalizeRequest::TPtr>> FinalizationRequestsQueue_;
};

}  // anonymous namespace

IActor* CreateKqpFinalizeScriptService(const NKikimrConfig::TFinalizeScriptServiceConfig& finalizeScriptServiceConfig,
    const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
    IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory) {
    return new TKqpFinalizeScriptService(finalizeScriptServiceConfig, metadataProviderConfig, std::move(federatedQuerySetupFactory));
}

}  // namespace NKikimr::NKqp
