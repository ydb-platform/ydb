#include "kqp_finalize_script_actor.h"

#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/kqp/federated_query/kqp_federated_query_actors.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_applicator_actor.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>


namespace NKikimr::NKqp {

namespace {

class TScriptFinalizerActor : public TActorBootstrapped<TScriptFinalizerActor> {
public:
    TScriptFinalizerActor(TEvScriptFinalizeRequest::TPtr request,
        const NKikimrConfig::TFinalizeScriptServiceConfig& finalizeScriptServiceConfig,
        const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
        : ReplyActor_(request->Sender)
        , ExecutionId_(request->Get()->Description.ExecutionId)
        , Database_(request->Get()->Description.Database)
        , FinalizationStatus_(request->Get()->Description.FinalizationStatus)
        , Request_(std::move(request))
        , FinalizationTimeout_(TDuration::Seconds(finalizeScriptServiceConfig.GetScriptFinalizationTimeoutSeconds()))
        , MaximalSecretsSnapshotWaitTime_(2 * TDuration::Seconds(metadataProviderConfig.GetRefreshPeriodSeconds()))
        , FederatedQuerySetup_(federatedQuerySetup)
    {}

    void Bootstrap() {
        Register(CreateSaveScriptFinalStatusActor(SelfId(), std::move(Request_)));
        Become(&TScriptFinalizerActor::FetchState);
    }

    STRICT_STFUNC(FetchState,
        hFunc(TEvSaveScriptFinalStatusResponse, Handle);
    )

    void Handle(TEvSaveScriptFinalStatusResponse::TPtr& ev) {
        if (!ev->Get()->ApplicateScriptExternalEffectRequired || ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Reply(ev->Get()->OperationAlreadyFinalized, ev->Get()->Status, std::move(ev->Get()->Issues));
            return;
        }

        Schedule(FinalizationTimeout_, new TEvents::TEvWakeup());
        Become(&TScriptFinalizerActor::PrepareState);

        CustomerSuppliedId_ = ev->Get()->CustomerSuppliedId;
        Sinks_ = std::move(ev->Get()->Sinks);
        UserToken_ = ev->Get()->UserToken;
        SecretNames_ = std::move(ev->Get()->SecretNames);

        if (Sinks_.empty()) {
            FinishScriptFinalization();
        } else if (SecretNames_.empty()) {
            ComputeScriptExternalEffect();
        } else {
            FetchSecrets();
        }
    }

private:
    STRICT_STFUNC(PrepareState,
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvDescribeSecretsResponse, Handle);
        hFunc(NFq::TEvents::TEvEffectApplicationResult, Handle);
    )

    void Handle(TEvents::TEvWakeup::TPtr&) {
        FinishScriptFinalization(Ydb::StatusIds::TIMEOUT, "Script finalization timeout");
    }

    void FetchSecrets() {
        RegisterDescribeSecretsActor(SelfId(), UserToken_, SecretNames_, ActorContext(), MaximalSecretsSnapshotWaitTime_);
    }

    void Handle(TEvDescribeSecretsResponse::TPtr& ev) {
        if (ev->Get()->Description.Status != Ydb::StatusIds::SUCCESS) {
            FinishScriptFinalization(ev->Get()->Description.Status, std::move(ev->Get()->Description.Issues));
            return;
        }

        FillSecureParams(ev->Get()->Description.SecretValues);
    }

    void FillSecureParams(const std::vector<TString>& secretValues) {
        std::map<TString, TString> secretsMap;
        for (size_t i = 0; i < secretValues.size(); ++i) {
            secretsMap.emplace(SecretNames_[i], secretValues[i]);
        }

        for (const auto& sink : Sinks_) {
            auto sinkName = sink.GetSinkName();

            if (sinkName) {
                const auto& structuredToken = NYql::CreateStructuredTokenParser(sink.GetAuthInfo()).ToBuilder().ReplaceReferences(secretsMap).ToJson();
                SecureParams_.emplace(sinkName, structuredToken);
            }
        }

        ComputeScriptExternalEffect();
    }

private:
    static void AddExternalEffectS3(const NKqpProto::TKqpExternalSink& sink, NYql::NDqProto::TExternalEffect& externalEffectS3) {
        NYql::NS3::TSink sinkSettings;
        sink.GetSettings().UnpackTo(&sinkSettings);

        NYql::NS3::TEffect sinkEffect;
        sinkEffect.SetToken(sink.GetSinkName());
        sinkEffect.MutableCleanup()->SetUrl(sinkSettings.GetUrl());
        sinkEffect.MutableCleanup()->SetPrefix(sinkSettings.GetPath());

        externalEffectS3.AddEffects()->SetData(sinkEffect.SerializeAsString());
    }

    void ComputeScriptExternalEffect() {
        NYql::NDqProto::TExternalEffect externalEffectS3;
        externalEffectS3.SetProviderName(TString(NYql::S3ProviderName));

        for (const auto& sink : Sinks_) {
            const TString& sinkType = sink.GetType();

            if (sinkType == "S3Sink") {
                AddExternalEffectS3(sink, externalEffectS3);
            } else {
                FinishScriptFinalization(Ydb::StatusIds::INTERNAL_ERROR, TStringBuilder() << "unknown effect sink type: " << sinkType);
                return;
            }
        }

        RunS3ApplicatorActor(externalEffectS3);
    }

    void RunS3ApplicatorActor(const NYql::NDqProto::TExternalEffect& externalEffect) {
        if (!FederatedQuerySetup_) {
            FinishScriptFinalization(Ydb::StatusIds::INTERNAL_ERROR, "unable to aplicate s3 external effect, invalid federated query setup");
            return;
        }

        Register(NYql::NDq::MakeS3ApplicatorActor(
            SelfId(),
            FederatedQuerySetup_->HttpGateway,
            CreateGuidAsString(),
            CustomerSuppliedId_,
            std::nullopt,
            FinalizationStatus_ == EFinalizationStatus::FS_COMMIT,
            THashMap<TString, TString>(SecureParams_.begin(), SecureParams_.end()),
            FederatedQuerySetup_->CredentialsFactory,
            externalEffect
        ).Release());
    }

    void Handle(NFq::TEvents::TEvEffectApplicationResult::TPtr& ev) {
        if (ev->Get()->FatalError) {
            FinishScriptFinalization(Ydb::StatusIds::BAD_REQUEST, std::move(ev->Get()->Issues));
        } else {
            FinishScriptFinalization();
        }
    }

private:
    STRICT_STFUNC(FinishState,
        IgnoreFunc(TEvents::TEvWakeup);
        IgnoreFunc(TEvDescribeSecretsResponse);
        IgnoreFunc(NFq::TEvents::TEvEffectApplicationResult);
        hFunc(TEvScriptExecutionFinished, Handle);
    )

    void FinishScriptFinalization(std::optional<Ydb::StatusIds::StatusCode> status, NYql::TIssues issues) {
        Register(CreateScriptFinalizationFinisherActor(SelfId(), ExecutionId_, Database_, status, std::move(issues)));
        Become(&TScriptFinalizerActor::FinishState);
    }

    void FinishScriptFinalization(Ydb::StatusIds::StatusCode status, const TString& message) {
        FinishScriptFinalization(status, { NYql::TIssue(message) });
    }

    void FinishScriptFinalization() {
        FinishScriptFinalization(std::nullopt, {});
    }

    void Handle(TEvScriptExecutionFinished::TPtr& ev) {
        Reply(ev->Get()->OperationAlreadyFinalized, ev->Get()->Status, std::move(ev->Get()->Issues));
    }

    void Reply(bool operationAlreadyFinalized, Ydb::StatusIds::StatusCode status, NYql::TIssues&& issues) {
        Send(ReplyActor_, new TEvScriptExecutionFinished(operationAlreadyFinalized, status, std::move(issues)));
        Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), new TEvScriptFinalizeResponse(ExecutionId_));

        PassAway();
    }

private:
    TActorId ReplyActor_;
    TString ExecutionId_;
    TString Database_;
    EFinalizationStatus FinalizationStatus_;
    TEvScriptFinalizeRequest::TPtr Request_;

    TDuration FinalizationTimeout_;
    TDuration MaximalSecretsSnapshotWaitTime_;
    const std::optional<TKqpFederatedQuerySetup>& FederatedQuerySetup_;

    TString CustomerSuppliedId_;
    std::vector<NKqpProto::TKqpExternalSink> Sinks_;

    TString UserToken_;
    std::vector<TString> SecretNames_;
    std::unordered_map<TString, TString> SecureParams_;
};

}  // anonymous namespace

IActor* CreateScriptFinalizerActor(TEvScriptFinalizeRequest::TPtr request,
    const NKikimrConfig::TFinalizeScriptServiceConfig& finalizeScriptServiceConfig,
    const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup) {
    return new TScriptFinalizerActor(std::move(request), finalizeScriptServiceConfig, metadataProviderConfig, federatedQuerySetup);
}

}  // namespace NKikimr::NKqp
