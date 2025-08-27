#include "metadata_helpers.h"

#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
using TAsyncStatus = NThreading::TFuture<TYqlConclusionStatus>;
using TExternalContext = NMetadata::NModifications::IOperationsManager::TExternalModificationContext;

class TFeatureFlagCheckResult {
public:
    void SetStatus(NYql::EYqlIssueCode status) {
        Status = status;
    }

    void AddIssue(NYql::TIssue issue) {
        Issues.AddIssue(std::move(issue));
    }

    void AddIssues(NYql::TIssues issues) {
        Issues.AddIssues(std::move(issues));
    }

    TFeatureFlagCheckResult& FromFeatureFlag(bool isEnabled, IFeatureFlagExtractor::TPtr extractor) {
        Issues.Clear();

        if (isEnabled) {
            Status = NYql::TIssuesIds::SUCCESS;
        } else {
            Status = NYql::TIssuesIds::KIKIMR_UNSUPPORTED;
            Issues.AddIssue(extractor->GetMessageOnDisabled());
        }

        return *this;
    }

public:
    NYql::EYqlIssueCode Status = NYql::TIssuesIds::SUCCESS;
    NYql::TIssues Issues;
};

}  // anonymous namespace

TAsyncStatus CheckFeatureFlag(ui32 nodeId, IFeatureFlagExtractor::TPtr extractor, const TExternalContext& context) {
    auto* actorSystem = context.GetActorSystem();
    if (!actorSystem) {
        return NThreading::MakeFuture<TYqlConclusionStatus>(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR, "Internal error. Object operation needs an actor system. Please contact internal support"));
    }

    using TRequest = NConsole::TEvConfigsDispatcher::TEvGetConfigRequest;
    using TResponse = NConsole::TEvConfigsDispatcher::TEvGetConfigResponse;
    auto event = std::make_unique<TRequest>((ui32)NKikimrConsole::TConfigItem::FeatureFlagsItem);

    auto promise = NThreading::NewPromise<TFeatureFlagCheckResult>();
    actorSystem->Register(new TActorRequestHandler<TRequest, TResponse, TFeatureFlagCheckResult>(
        NConsole::MakeConfigsDispatcherID(nodeId), event.release(), promise,
        [extractor](NThreading::TPromise<TFeatureFlagCheckResult> promise, TResponse&& response) {
            promise.SetValue(TFeatureFlagCheckResult()
                .FromFeatureFlag(extractor->IsEnabled(response.Config->GetFeatureFlags()), extractor)
            );
        }
    ));

    return promise.GetFuture().Apply([actorSystem, extractor](const NThreading::TFuture<TFeatureFlagCheckResult>& f) {
        auto result = f.GetValue();
        if (result.Status == NYql::TIssuesIds::KIKIMR_TEMPORARILY_UNAVAILABLE) {
            // Fallback if CMS is unavailable
            result.FromFeatureFlag(extractor->IsEnabled(AppData(actorSystem)->FeatureFlags), extractor);
        }
        if (result.Status == NYql::TIssuesIds::SUCCESS) {
            return TYqlConclusionStatus::Success();
        }
        return TYqlConclusionStatus::Fail(result.Status, result.Issues.ToString());
    });
}

TAsyncStatus ChainFeatures(TAsyncStatus lastFeature, std::function<TAsyncStatus()> callback) {
    return lastFeature.Apply([callback](const TAsyncStatus& f) {
        auto status = f.GetValue();
        if (status.IsFail()) {
            return NThreading::MakeFuture(status);
        }
        return callback();
    });
}

TAsyncStatus SendSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const TExternalContext& context) {
    auto request = std::make_unique<TEvTxUserProxy::TEvProposeTransaction>();
    request->Record.SetDatabaseName(context.GetDatabase());
    if (context.GetUserToken()) {
        request->Record.SetUserToken(context.GetUserToken()->GetSerializedToken());
    }
    *request->Record.MutableTransaction()->MutableModifyScheme() = schemeTx;

    auto promise = NThreading::NewPromise<NKqp::TSchemeOpRequestHandler::TResult>();
    context.GetActorSystem()->Register(new TSchemeOpRequestHandler(request.release(), promise, schemeTx.GetFailedOnAlreadyExists(), schemeTx.GetSuccessOnNotExist()));

    return promise.GetFuture().Apply([operationType = schemeTx.GetOperationType()](const NThreading::TFuture<NKqp::TSchemeOpRequestHandler::TResult>& f) {
        try {
            auto response = f.GetValue();
            if (response.Success()) {
                return TYqlConclusionStatus::Success();
            }
            return TYqlConclusionStatus::Fail(response.Status(), TStringBuilder() << "Failed to execute " << NKikimrSchemeOp::EOperationType_Name(operationType) << ": " << response.Issues().ToString());
        } catch (...) {
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_SCHEME_ERROR, TStringBuilder() << "Failed to execute " << NKikimrSchemeOp::EOperationType_Name(operationType) << ", got scheme error: " << CurrentExceptionMessage());
        }
    });
}

}  // namespace NKikimr::NKqp
