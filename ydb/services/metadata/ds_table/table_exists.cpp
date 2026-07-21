#include "table_exists.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::METADATA_PROVIDER

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

void TTableExistsActor::OnBootstrap() {
    Become(&TTableExistsActor::StateMain);

    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = AppData()->TenantName;
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.Path = NKikimr::SplitPath(Path);
    YDB_LOG_DEBUG("Dump selfId, sendTo",
        {"selfId", SelfId()},
        {"sendTo", MakeSchemeCacheID()});
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), IEventHandle::FlagTrackDelivery);
}

void TTableExistsActor::Handle(NActors::TEvents::TEvUndelivered::TPtr& /*ev*/) {
    YDB_LOG_DEBUG("Dump actor, event, selfId, sendTo",
        {"actor", "TTableExistsActor"},
        {"event", "undelivered"},
        {"selfId", SelfId()},
        {"sendTo", MakeSchemeCacheID()});
    OutputController->OnPathExistsCheckFailed("scheme_cache_undelivered_message", Path);
}

void TTableExistsActor::OnTimeout() {
    YDB_LOG_DEBUG("Dump actor, event, selfId, sendTo",
        {"actor", "TTableExistsActor"},
        {"event", "timeout"},
        {"selfId", SelfId()},
        {"sendTo", MakeSchemeCacheID()});
    OutputController->OnPathExistsCheckFailed("timeout", Path);
}

}
