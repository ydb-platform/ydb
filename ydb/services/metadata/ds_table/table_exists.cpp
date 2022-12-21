#include "table_exists.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

namespace NKikimr::NMetadata::NProvider {

void TTableExistsActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    auto* info = ev->Get();
    const auto& request = info->Request;
    auto g = PassAwayGuard();

    if (request->ResultSet.empty()) {
        OutputController->OnPathExistsCheckFailed("navigation problems for path", Path);
        return;
    }
    if (request->ResultSet.size() != 1) {
        OutputController->OnPathExistsCheckFailed("cannot resolve database path", Path);
        return;
    }
    auto& entity = request->ResultSet.front();
    if (entity.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        if (entity.Kind == NSchemeCache::TSchemeCacheNavigate::EKind::KindTable) {
            OutputController->OnPathExistsCheckResult(true, Path);
        } else {
            OutputController->OnPathExistsCheckResult(false, Path);
        }
    } else if (entity.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath) {
        OutputController->OnPathExistsCheckResult(false, Path);
    } else if (entity.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable) {
        OutputController->OnPathExistsCheckResult(false, Path);
    } else if (entity.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown) {
        OutputController->OnPathExistsCheckResult(false, Path);
    } else {
        OutputController->OnPathExistsCheckFailed("incorrect path status: " + ::ToString(entity.Status), Path);
    }
}

NKikimrServices::TActivity::EType TTableExistsActor::ActorActivityType() {
    return NKikimrServices::TActivity::METADATA_SCHEME_DESCRIPTION_ACTOR;
}

void TTableExistsActor::OnBootstrap() {
    Become(&TTableExistsActor::StateMain);

    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = NKikimr::CanonizePath(AppData()->TenantName);
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
    entry.Path = NKikimr::SplitPath(Path);
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

void TTableExistsActor::OnTimeout() {
    OutputController->OnPathExistsCheckFailed("timeout", Path);
}

}
