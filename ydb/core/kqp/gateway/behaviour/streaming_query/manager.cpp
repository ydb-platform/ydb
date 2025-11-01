#include "manager.h"
#include "queries.h"

#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/protos/feature_flags.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = TStreamingQueryManager::TYqlConclusionStatus;
using TAsyncStatus = TStreamingQueryManager::TAsyncStatus;

template <typename TValue>
using TYqlConclusion = TConclusionImpl<TYqlConclusionStatus, TValue>;

struct TFeatureFlagExtractor : public IFeatureFlagExtractor {
    bool IsEnabled(const NKikimrConfig::TFeatureFlags& flags) const override {
        return flags.GetEnableStreamingQueries();
    }

    bool IsEnabled(const TFeatureFlags& flags) const override {
        return flags.GetEnableStreamingQueries();
    }

    TString GetMessageOnDisabled() const override {
        return "Streaming queries are disabled. Please contact your system administrator to enable it";
    }
};

[[nodiscard]] TYqlConclusionStatus FillStreamingQueryDesc(NKikimrSchemeOp::TStreamingQueryDescription& streamingQueryDesc, const TString& name, const NYql::TObjectSettingsImpl& settings) {
    streamingQueryDesc.SetName(name);

    auto& featuresExtractor = settings.GetFeaturesExtractor();
    auto& properties = *streamingQueryDesc.MutableProperties()->MutableProperties();

    // Validation of features values will be performed on execution step
    for (const auto& property : {
        TStreamingQueryConfig::TSqlSettings::QUERY_TEXT_FEATURE,
        TStreamingQueryConfig::TProperties::Run,
        TStreamingQueryConfig::TProperties::ResourcePool,
        TStreamingQueryConfig::TProperties::Force,
    }) {
        if (const auto& value = featuresExtractor.Extract(property)) {
            if (!properties.emplace(property, *value).second) {
                return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Duplicate property " << property);
            }
        }
    }

    if (!featuresExtractor.IsFinished()) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Unknown property: " << featuresExtractor.GetRemainedParamsString());
    }

    return TYqlConclusionStatus::Success();
}

TYqlConclusion<std::pair<TString, TString>> SplitPath(const TString& tableName, const TString& database, bool createDir) {
    std::pair<TString, TString> pathPair;
    TString error;
    if (!NSchemeHelpers::SplitTablePath(tableName, database, pathPair, error, createDir)) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, TStringBuilder() << "Invalid streaming query path: " << error);
    }
    return pathPair;
}

class TObjectOperationController : public IStreamingQueryOperationController {
public:
    explicit TObjectOperationController(NThreading::TPromise<TYqlConclusionStatus> promise)
        : Promise(std::move(promise))
    {}

    void OnAlteringProblem(const TString& errorMessage) override {
        Promise.SetValue(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, errorMessage));
    }

    void OnAlteringFinished() override {
        Promise.SetValue(TYqlConclusionStatus::Success());
    }

    void OnAlteringFinishedWithStatus(const TYqlConclusionStatus& status) override {
        Promise.SetValue(status);
    }

private:
    NThreading::TPromise<TYqlConclusionStatus> Promise;
};

class TObjectOperationCommand : public NMetadata::NModifications::IObjectModificationCommand {
    using TBase = NMetadata::NModifications::IObjectModificationCommand;

public:
    TObjectOperationCommand(const NKqpProto::TKqpSchemeOperation& schemeOperation, NMetadata::IClassBehaviour::TPtr behaviour, IStreamingQueryOperationController::TPtr controller, const TStreamingQueryManager::TExternalModificationContext& context)
        : TBase(std::vector<NMetadata::NInternal::TTableRecord>{}, std::move(behaviour), controller, TStreamingQueryManager::TInternalModificationContext(context))
        , SchemeOperation(schemeOperation)
        , Controller(std::move(controller))
    {}

    void DoExecute() const override {
        try {
            switch (SchemeOperation.GetOperationCase()) {
                case NKqpProto::TKqpSchemeOperation::kCreateStreamingQuery:
                    DoCreateStreamingQuery(SchemeOperation.GetCreateStreamingQuery(), Controller, GetContext().GetExternalData());
                    break;
                case NKqpProto::TKqpSchemeOperation::kAlterStreamingQuery:
                    DoAlterStreamingQuery(SchemeOperation.GetAlterStreamingQuery(), Controller, GetContext().GetExternalData());
                    break;
                case NKqpProto::TKqpSchemeOperation::kDropStreamingQuery:
                    DoDropStreamingQuery(SchemeOperation.GetDropStreamingQuery(), Controller, GetContext().GetExternalData());
                    break;
                default:
                    Controller->OnAlteringProblem(TStringBuilder() << "Internal error. Execution of prepared operation for STREAMING_QUERY object: unsupported operation: " << static_cast<i32>(SchemeOperation.GetOperationCase()));
                    break;
            }
        } catch (...) {
            Controller->OnAlteringProblem(TStringBuilder() << "Internal error. Got unexpected exception during execution of STREAMING_QUERY modification operation: " << CurrentExceptionMessage());
        }
    }

private:
    const NKqpProto::TKqpSchemeOperation SchemeOperation;
    const IStreamingQueryOperationController::TPtr Controller;
};

}  // anonymous namespace

TAsyncStatus TStreamingQueryManager::DoModify(const NYql::TObjectSettingsImpl& settings, ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    NKqpProto::TKqpSchemeOperation schemeOperation;
    if (const auto& status = DoPrepare(schemeOperation, settings, manager, context); status.IsFail()) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(status);
    }

    return ExecutePrepared(schemeOperation, nodeId, manager, context.GetExternalData());
}

TYqlConclusionStatus TStreamingQueryManager::DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const NMetadata::IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const {
    Y_UNUSED(manager);    

    try {
        switch (context.GetActivityType()) {
            case EActivityType::Create:
                return PrepareCreateStreamingQuery(schemeOperation, settings, context);
            case EActivityType::Alter:
                return PrepareAlterStreamingQuery(schemeOperation, settings, context);
            case EActivityType::Drop:
                return PrepareDropStreamingQuery(schemeOperation, settings, context);
            case EActivityType::Undefined:
                return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. Undefined operation for STREAMING_QUERY object");
            case EActivityType::Upsert:
                return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNIMPLEMENTED, "Upsert operation for STREAMING_QUERY objects is not implemented");
        }
    } catch (...) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, TStringBuilder() << "Internal error. Got unexpected exception during preparation of STREAMING_QUERY modification operation: " << CurrentExceptionMessage());
    }
}

TYqlConclusionStatus TStreamingQueryManager::PrepareCreateStreamingQuery(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context) {
    if (settings.GetExistingOk() && settings.GetReplaceIfExists()) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, "Options 'OR REPLACE' and 'IF NOT EXISTS' can not be used together for STREAMING_QUERY objects");
    }

    auto pathPairStatus = SplitPath(settings.GetObjectId(), context.GetExternalData().GetDatabase(), /* createDir */ true);
    if (pathPairStatus.IsFail()) {
        return pathPairStatus;
    }
    const auto& [workingDir, name] = pathPairStatus.DetachResult();

    auto& schemeTx = *schemeOperation.MutableCreateStreamingQuery();
    schemeTx.SetWorkingDir(workingDir);
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateStreamingQuery);
    schemeTx.SetFailedOnAlreadyExists(!settings.GetExistingOk());
    schemeTx.SetReplaceIfExists(settings.GetReplaceIfExists());

    return FillStreamingQueryDesc(*schemeTx.MutableCreateStreamingQuery(), name, settings);
}

TYqlConclusionStatus TStreamingQueryManager::PrepareAlterStreamingQuery(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context) {
    auto pathPairStatus = SplitPath(settings.GetObjectId(), context.GetExternalData().GetDatabase(), /* createDir */ false);
    if (pathPairStatus.IsFail()) {
        return pathPairStatus;
    }
    const auto& [workingDir, name] = pathPairStatus.DetachResult();

    auto& schemeTx = *schemeOperation.MutableAlterStreamingQuery();
    schemeTx.SetWorkingDir(workingDir);
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterStreamingQuery);
    schemeTx.SetSuccessOnNotExist(settings.GetMissingOk());

    return FillStreamingQueryDesc(*schemeTx.MutableCreateStreamingQuery(), name, settings);
}

TYqlConclusionStatus TStreamingQueryManager::PrepareDropStreamingQuery(NKqpProto::TKqpSchemeOperation& schemeOperation, const NYql::TObjectSettingsImpl& settings, const TInternalModificationContext& context) {
    auto pathPairStatus = SplitPath(settings.GetObjectId(), context.GetExternalData().GetDatabase(), /* createDir */ false);
    if (pathPairStatus.IsFail()) {
        return pathPairStatus;
    }
    const auto& [workingDir, name] = pathPairStatus.DetachResult();

    auto& schemeTx = *schemeOperation.MutableDropStreamingQuery();
    schemeTx.SetWorkingDir(workingDir);
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropStreamingQuery);
    schemeTx.SetSuccessOnNotExist(settings.GetMissingOk());

    schemeTx.MutableDrop()->SetName(name);

    return TYqlConclusionStatus::Success();
}

TAsyncStatus TStreamingQueryManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation, const ui32 nodeId, const NMetadata::IClassBehaviour::TPtr& manager, const TExternalModificationContext& context) const {
    if (!context.GetDatabaseId()) {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. Missing DatabaseId for STREAMING_QUERY object operation"));
    }

    if (!context.GetActorSystem()) {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. STREAMING_QUERY object operation needs an actor system. Please contact internal support"));
    }

    TAsyncStatus validationFeature = NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Success());
    if (schemeOperation.GetOperationCase() != NKqpProto::TKqpSchemeOperation::kDropStreamingQuery) {
        validationFeature = CheckFeatureFlag(nodeId, MakeIntrusive<TFeatureFlagExtractor>(), context);
    }

    return ChainFeatures(validationFeature, [schemeOperation, nodeId, manager, context]() {
        auto promise = NThreading::NewPromise<TYqlConclusionStatus>();
        context.GetActorSystem()->Send(NMetadata::NProvider::MakeServiceId(nodeId),  new NMetadata::NProvider::TEvObjectsOperation(
            std::make_shared<TObjectOperationCommand>(schemeOperation, manager, std::make_shared<TObjectOperationController>(promise), context)
        ));
        return promise.GetFuture();
    });
}

}  // namespace NKikimr::NKqp
