#include "operation_helpers.h"
#include "rpc_calls.h"

#include "rpc_export_base.h"
#include "rpc_import_base.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/core/tx/schemeshard/schemeshard_build_index.h>
#include <ydb/core/tx/schemeshard/schemeshard_export.h>
#include <ydb/core/tx/schemeshard/schemeshard_import.h>

#include <ydb/core/protos/index_builder.pb.h>

#include <ydb/public/lib/operation_id/protos/operation_id.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr {
namespace NGRpcService {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_PROXY, LogPrefix << stream)

IEventBase* CreateNavigateForPath(const TString& path) {
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

    request->DatabaseName = path;

    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.Path = ::NKikimr::SplitPath(path);
    entry.RedirectRequired = false;

    return new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release());
}

TActorId CreatePipeClient(ui64 id, const TActorContext& ctx) {
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {.RetryLimitCount = 3};
    return ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, id, clientConfig));
}

Ydb::TOperationId ToOperationId(const NKikimrIndexBuilder::TIndexBuild& build) {
    Ydb::TOperationId operationId;
    operationId.SetKind(Ydb::TOperationId::BUILD_INDEX);
    NOperationId::AddOptionalValue(operationId, "id", ToString(build.GetId()));

    return operationId;
}

void ToOperation(const NKikimrIndexBuilder::TIndexBuild& build, Ydb::Operations::Operation* operation) {
    operation->set_id(NOperationId::ProtoToString(ToOperationId(build)));
    operation->mutable_issues()->CopyFrom(build.GetIssues());

    switch (build.GetState()) {
        case Ydb::Table::IndexBuildState::STATE_DONE:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
        break;
        case Ydb::Table::IndexBuildState::STATE_CANCELLED:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::CANCELLED);
        break;
        case Ydb::Table::IndexBuildState::STATE_REJECTED:
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::ABORTED);
        break;
        default:
            operation->set_ready(false);
    }

    Ydb::Table::IndexBuildMetadata metadata;
    metadata.set_state(build.GetState());
    metadata.set_progress(build.GetProgress());
    auto desc = metadata.mutable_description();
    desc->set_path(build.GetSettings().source_path());
    desc->mutable_index()->CopyFrom(build.GetSettings().index());

    auto data = operation->mutable_metadata();
    data->PackFrom(metadata);
}

bool TryGetId(const NOperationId::TOperationId& operationId, ui64& id) {
    const auto& ids = operationId.GetValue("id");

    if (ids.size() != 1) {
        return false;
    }

    if (!TryFromString(*ids[0], id)) {
        return false;
    }

    return id;
}


} // namespace NGRpcService
} // namespace NKikimr
