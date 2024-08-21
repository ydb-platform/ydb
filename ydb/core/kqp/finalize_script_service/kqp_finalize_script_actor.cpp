#include "kqp_finalize_script_actor.h"

#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/events/events.h>

#include <ydb/core/kqp/federated_query/kqp_federated_query_actors.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>

#include <ydb/core/tx/datashard/const.h>

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/structured_token/yql_token_builder.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_applicator_actor.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>


namespace NKikimr::NKqp {

namespace {

class TScriptFinalizerActor : public TActorBootstrapped<TScriptFinalizerActor> {
    static constexpr size_t MAX_ARTIFACTS_SIZE_BYTES = 40_MB;

public:
    TScriptFinalizerActor(TEvScriptFinalizeRequest::TPtr request,
        const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
        std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactor)
        : ReplyActor(request->Sender)
        , ExecutionId(request->Get()->Description.ExecutionId)
        , Database(request->Get()->Description.Database)
        , FinalizationStatus(request->Get()->Description.FinalizationStatus)
        , Request(std::move(request))
        , FinalizationTimeout(TDuration::Seconds(queryServiceConfig.GetFinalizeScriptServiceConfig().GetScriptFinalizationTimeoutSeconds()))
        , FederatedQuerySetup(federatedQuerySetup)
        , Compressor(queryServiceConfig.GetQueryArtifactsCompressionMethod(), queryServiceConfig.GetQueryArtifactsCompressionMinSize())
        , S3ActorsFactor(std::move(s3ActorsFactor))
    {}

    void CompressScriptArtifacts() const {
        auto& description = Request->Get()->Description;

        TString astTruncateDescription;
        if (size_t planSize = description.QueryPlan.value_or("").size(); description.QueryAst && description.QueryAst->size() + planSize > MAX_ARTIFACTS_SIZE_BYTES) {
            astTruncateDescription = TStringBuilder() << "Query artifacts size is " << description.QueryAst->size() + planSize << " bytes (plan + ast), that is larger than allowed limit " << MAX_ARTIFACTS_SIZE_BYTES << " bytes, ast was truncated";
            size_t toRemove = std::min(description.QueryAst->size() + planSize - MAX_ARTIFACTS_SIZE_BYTES, description.QueryAst->size());
            description.QueryAst = TruncateString(*description.QueryAst, description.QueryAst->size() - toRemove);
        }

        auto ast = description.QueryAst;
        if (Compressor.IsEnabled() && ast) {
            const auto& [astCompressionMethod, astCompressed] = Compressor.Compress(*ast);
            description.QueryAstCompressionMethod = astCompressionMethod;
            description.QueryAst = astCompressed;
        }

        if (description.QueryAst && description.QueryAst->size() > NDataShard::NLimits::MaxWriteValueSize) {
            astTruncateDescription = TStringBuilder() << "Query ast size is " << description.QueryAst->size() << " bytes, that is larger than allowed limit " << NDataShard::NLimits::MaxWriteValueSize << " bytes, ast was truncated";
            description.QueryAst = TruncateString(*ast, NDataShard::NLimits::MaxWriteValueSize - 1_KB);
            description.QueryAstCompressionMethod = std::nullopt;
        }

        if (astTruncateDescription) {
            NYql::TIssue astTruncatedIssue(astTruncateDescription);
            astTruncatedIssue.SetCode(NYql::DEFAULT_ERROR, NYql::TSeverityIds::S_INFO);
            description.Issues.AddIssue(astTruncatedIssue);
        }
    }

    void Bootstrap() {
        CompressScriptArtifacts();
        Register(CreateSaveScriptFinalStatusActor(SelfId(), std::move(Request)));
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

        Schedule(FinalizationTimeout, new TEvents::TEvWakeup());
        Become(&TScriptFinalizerActor::PrepareState);

        CustomerSuppliedId = ev->Get()->CustomerSuppliedId;
        Sinks = std::move(ev->Get()->Sinks);
        UserToken = ev->Get()->UserToken;
        SecretNames = std::move(ev->Get()->SecretNames);

        if (Sinks.empty()) {
            FinishScriptFinalization();
        } else if (SecretNames.empty()) {
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
        RegisterDescribeSecretsActor(SelfId(), UserToken, SecretNames, ActorContext().ActorSystem());
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
            secretsMap.emplace(SecretNames[i], secretValues[i]);
        }

        for (const auto& sink : Sinks) {
            const auto& sinkName = sink.GetSinkName();

            if (sinkName) {
                const auto& structuredToken = NYql::CreateStructuredTokenParser(sink.GetAuthInfo()).ToBuilder().ReplaceReferences(secretsMap).ToJson();
                SecureParams.emplace(sinkName, structuredToken);
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

        for (const auto& sink : Sinks) {
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
        if (!FederatedQuerySetup) {
            FinishScriptFinalization(Ydb::StatusIds::INTERNAL_ERROR, "unable to aplicate s3 external effect, invalid federated query setup");
            return;
        }

        Register(S3ActorsFactor->CreateS3ApplicatorActor(
            SelfId(),
            FederatedQuerySetup->HttpGateway,
            CreateGuidAsString(),
            CustomerSuppliedId,
            std::nullopt,
            FinalizationStatus == EFinalizationStatus::FS_COMMIT,
            THashMap<TString, TString>(SecureParams.begin(), SecureParams.end()),
            FederatedQuerySetup->CredentialsFactory,
            externalEffect
        ).Release());
    }

    void Handle(NFq::TEvents::TEvEffectApplicationResult::TPtr& ev) {
        if (ev->Get()->FatalError) {
            NYql::TIssue rootIssue("Failed to commit/abort s3 multipart uploads");
            for (const NYql::TIssue& issue : ev->Get()->Issues) {
                rootIssue.AddSubIssue(MakeIntrusive<NYql::TIssue>(issue));
            }
            FinishScriptFinalization(Ydb::StatusIds::BAD_REQUEST, {rootIssue});
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
        Register(CreateScriptFinalizationFinisherActor(SelfId(), ExecutionId, Database, status, std::move(issues)));
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
        Send(ReplyActor, new TEvScriptExecutionFinished(operationAlreadyFinalized, status, std::move(issues)));
        Send(MakeKqpFinalizeScriptServiceId(SelfId().NodeId()), new TEvScriptFinalizeResponse(ExecutionId));

        PassAway();
    }

private:
    static TString TruncateString(const TString& str, size_t size) {
        return str.substr(0, std::min(str.size(), size)) + "...\n(TRUNCATED)";
    }

private:
    const TActorId ReplyActor;
    const TString ExecutionId;
    const TString Database;
    const EFinalizationStatus FinalizationStatus;
    TEvScriptFinalizeRequest::TPtr Request;

    const TDuration FinalizationTimeout;
    const std::optional<TKqpFederatedQuerySetup>& FederatedQuerySetup;
    const NFq::TCompressor Compressor;
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> S3ActorsFactor;

    TString CustomerSuppliedId;
    std::vector<NKqpProto::TKqpExternalSink> Sinks;

    TString UserToken;
    std::vector<TString> SecretNames;
    std::unordered_map<TString, TString> SecureParams;
};

}  // anonymous namespace

IActor* CreateScriptFinalizerActor(TEvScriptFinalizeRequest::TPtr request,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup,
    std::shared_ptr<NYql::NDq::IS3ActorsFactory> s3ActorsFactory
    ) {
    return new TScriptFinalizerActor(std::move(request), queryServiceConfig, federatedQuerySetup, std::move(s3ActorsFactory));
}

}  // namespace NKikimr::NKqp
