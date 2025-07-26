#include "kqp_finalize_script_service.h"
#include "kqp_finalize_script_actor.h"
#include "kqp_check_script_lease_actor.h"

#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/table_creator/table_creator.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>

#include <queue>

namespace NKikimr::NKqp {

namespace {

class TKqpFinalizeScriptService : public TActorBootstrapped<TKqpFinalizeScriptService> {
    using TRetryPolicy = IRetryPolicy<bool>;

public:
    TKqpFinalizeScriptService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
        IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory,
        bool enableBackgroundLeaseChecks)
        : QueryServiceConfig(queryServiceConfig)
        , EnableBackgroundLeaseChecks(enableBackgroundLeaseChecks)
        , FederatedQuerySetupFactory(federatedQuerySetupFactory)
        , S3ActorsFactory(std::move(s3ActorsFactory))
    {}

    void Bootstrap(const TActorContext& ctx) {
        Counters = MakeIntrusive<TKqpCounters>(AppData()->Counters, &TlsActivationContext->AsActorContext());

        if (FederatedQuerySetupFactory) {
            FederatedQuerySetup = FederatedQuerySetupFactory->Make(ctx.ActorSystem());
        }

        Become(&TKqpFinalizeScriptService::MainState);

        if (EnableBackgroundLeaseChecks) {
            CheckScriptExecutionTablesExistence();
        }
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

    void StartScriptExecutionBackgroundChecks() {
        if (!EnableBackgroundLeaseChecks || ScriptExecutionLeaseCheckActor) {
            return;
        }

        ScriptExecutionLeaseCheckActor = Register(CreateScriptExecutionLeaseCheckActor(QueryServiceConfig, Counters));
    }

    STRICT_STFUNC(MainState,
        hFunc(TEvSaveScriptExternalEffectRequest, Handle);
        hFunc(TEvScriptFinalizeRequest, Handle);
        hFunc(TEvScriptFinalizeResponse, Handle);
        sFunc(TEvStartScriptExecutionBackgroundChecks, StartScriptExecutionBackgroundChecks);

        hFunc(TEvents::TEvUndelivered, Handle)
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        sFunc(TEvents::TEvWakeup, CheckScriptExecutionTablesExistence);
    )

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
    void CheckScriptExecutionTablesExistence() const {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(NTableCreator::BuildSchemeCacheNavigateRequest({
            {".metadata", "script_executions"},
            {".metadata", "script_execution_leases"},
            {".metadata", "result_sets"},
        }).Release()), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Failed to check script execution tables existence, got undelivered to scheme cache: " << ev->Get()->Reason);
        Retry();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
        Y_ABORT_UNLESS(request.ResultSet.size() == 3);

        for (const auto& result : request.ResultSet) {
            if (result.Status != EStatus::Ok) {
                LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Failed to check script execution tables existence, scheme status: " << result.Status << ", path: " << JoinPath(result.Path));
            }

            switch (result.Status) {
                case EStatus::Unknown:
                case EStatus::PathNotTable:
                case EStatus::PathNotPath:
                case EStatus::AccessDenied:
                case EStatus::RedirectLookupError:
                case EStatus::RootUnknown:
                case EStatus::PathErrorUnknown:
                    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Script execution table " << JoinPath(result.Path) << " not found");
                    return;
                case EStatus::LookupError:
                case EStatus::TableCreationNotComplete:
                    Retry(true);
                    return;
                case EStatus::Ok:
                    break;
            }
        }

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Start script execution background checks");
        StartScriptExecutionBackgroundChecks();
    }

    void Retry(bool longDelay = false) {
        if (!RetryState) {
            RetryState = TRetryPolicy::GetExponentialBackoffPolicy(
                [](bool longDelay) {
                    return longDelay ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;
                },
                TDuration::MilliSeconds(100),
                TDuration::MilliSeconds(300),
                TDuration::Minutes(1),
                std::numeric_limits<size_t>::max(),
                TDuration::Max()
            )->CreateRetryState();
        }

        if (const auto delay = RetryState->GetNextRetryDelay(longDelay)) {
            Schedule(*delay, new NActors::TEvents::TEvWakeup());
        } else {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Failed to check script execution tables existence, retry limit exceeded");
        }
    }

private:
    TString LogPrefix() const {
        return TStringBuilder() << "[ScriptExecutions] [TKqpFinalizeScriptService] ";
    }

private:
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const bool EnableBackgroundLeaseChecks = true;

    TIntrusivePtr<TKqpCounters> Counters;
    IKqpFederatedQuerySetupFactory::TPtr FederatedQuerySetupFactory;
    std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;
    TRetryPolicy::IRetryState::TPtr RetryState;  // Used for check script execution tables existence
    TActorId ScriptExecutionLeaseCheckActor;

    ui32 FinalizationRequestsInFlight = 0;
    std::queue<TString> WaitingFinalizationExecutions;
    std::unordered_map<TString, std::vector<TEvScriptFinalizeRequest::TPtr>> FinalizationRequestsQueue;

    std::shared_ptr<NYql::NDq::IS3ActorsFactory> S3ActorsFactory;
};

}  // anonymous namespace

IActor* CreateKqpFinalizeScriptService(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    IKqpFederatedQuerySetupFactory::TPtr federatedQuerySetupFactory,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory,
    bool enableBackgroundLeaseChecks) {
    return new TKqpFinalizeScriptService(queryServiceConfig, std::move(federatedQuerySetupFactory), std::move(s3ActorsFactory), enableBackgroundLeaseChecks);
}

}  // namespace NKikimr::NKqp
