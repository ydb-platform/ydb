#include "ss_dialog.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

namespace NKikimr::NMetadata::NInternal {

void TSSDialogActor::Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& /*ev*/) {
    OnFail("cannot delivery message to schemeshard");
    PassAway();
}

void TSSDialogActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    auto* info = ev->Get();
    const auto& request = info->Request;

    if (request->ResultSet.empty()) {
        OnFail("navigation problems");
        PassAway();
        return;
    }
    if (request->ResultSet.size() != 1) {
        OnFail("cannot resolve database path");
        PassAway();
        return;
    }
    auto& entity = request->ResultSet.front();
    SchemeShardId = entity.DomainInfo->ExtractSchemeShard();
    NTabletPipe::TClientConfig clientConfig;
    SchemeShardPipe = MakePipePerNodeCacheID(false);
    Execute();
}

void TSSDialogActor::OnBootstrap() {
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = NKikimr::CanonizePath(AppData()->TenantName);
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.Path = NKikimr::SplitPath(request->DatabaseName);
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

void TSSDialogActor::Die(const TActorContext& ctx) {
    NTabletPipe::CloseAndForgetClient(SelfId(), SchemeShardPipe);
    return IActor::Die(ctx);
}

void TSSDialogActor::OnTimeout() {
    OnFail("timeout");
}

}
