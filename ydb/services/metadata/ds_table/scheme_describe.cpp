#include "scheme_describe.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NMetadata::NProvider {

void TSchemeDescriptionActor::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    auto* info = ev->Get();
    const auto& request = info->Request;

    if (request->ResultSet.empty()) {
        Controller->OnDescriptionFailed("navigation problems for path " + Path, RequestId);
        PassAway();
        return;
    }
    if (request->ResultSet.size() != 1) {
        Controller->OnDescriptionFailed("cannot resolve database path " + Path, RequestId);
        PassAway();
        return;
    }
    auto& entity = request->ResultSet.front();
    auto g = PassAwayGuard();
    Controller->OnDescriptionSuccess(std::move(entity.Columns), RequestId);
}

void TSchemeDescriptionActor::Bootstrap() {
    Become(&TSchemeDescriptionActor::StateMain);

    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = NKikimr::CanonizePath(AppData()->TenantName);
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
    entry.Path = NKikimr::SplitPath(Path);
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

NKikimrServices::TActivity::EType TSchemeDescriptionActor::ActorActivityType() {
    return NKikimrServices::TActivity::METADATA_SCHEME_DESCRIPTION_ACTOR;
}

}
