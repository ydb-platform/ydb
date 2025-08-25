#include "metadata_helpers.h"

#include <ydb/core/kqp/gateway/actors/scheme.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = NMetadata::NModifications::IOperationsManager::TYqlConclusionStatus;
using TAsyncStatus = NThreading::TFuture<TYqlConclusionStatus>;

}  // anonymous namespace

TAsyncStatus ChainFeatures(TAsyncStatus lastFeature, std::function<TAsyncStatus()> callback) {
    return lastFeature.Apply([callback](const TAsyncStatus& f) {
        auto status = f.GetValue();
        if (status.IsFail()) {
            return NThreading::MakeFuture(status);
        }
        return callback();
    });
}

TAsyncStatus SendSchemeRequest(const NKikimrSchemeOp::TModifyScheme& schemeTx, const NMetadata::NModifications::IOperationsManager::TExternalModificationContext& context) {
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
