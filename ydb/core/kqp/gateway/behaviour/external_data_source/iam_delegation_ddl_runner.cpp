#include "iam_delegation_ddl.h"

#include "iam_delegation_ddl_actor.h"

namespace NKikimr::NKqp::NExternalDataSource {
namespace {

using TContext = TExternalDataSourceManager::TExternalModificationContext;
using TStatus = TExternalDataSourceManager::TYqlConclusionStatus;
using TAsyncStatus = TExternalDataSourceManager::TAsyncStatus;

} // anonymous namespace

TAsyncStatus ExecuteIamDelegationDdl(
    const NKikimrSchemeOp::TModifyScheme& schemeTx,
    const TContext& context,
    NKqpProto::TKqpSchemeOperation::OperationCase operationCase)
{
    auto* actorSystem = context.GetActorSystem();
    if (!actorSystem) {
        return NThreading::MakeFuture(TStatus::Fail(
            NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            "IAM delegation DDL requires an actor system"));
    }
    auto promise = NThreading::NewPromise<TStatus>();
    auto future = promise.GetFuture();
    auto* actor = CreateIamDelegationDdlActor(
        schemeTx, context, operationCase, std::move(promise));
    if (!actor) {
        return NThreading::MakeFuture(TStatus::Fail(
            NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            "Unsupported EXTERNAL_DATA_SOURCE operation"));
    }
    actorSystem->Register(actor);
    return future;
}

} // namespace NKikimr::NKqp::NExternalDataSource
