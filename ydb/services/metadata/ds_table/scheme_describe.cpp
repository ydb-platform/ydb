#include "scheme_describe.h"

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/ydb_convert/table_description.h>

namespace NKikimr::NMetadata::NProvider {

void TSchemeDescriptionActorImpl::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    auto* info = ev->Get();
    const auto& request = info->Request;
    auto g = PassAwayGuard();

    if (request->ResultSet.empty()) {
        Controller->OnDescriptionFailed("navigation problems for " + DebugString(), RequestId);
        return;
    }
    if (request->ResultSet.size() != 1) {
        Controller->OnDescriptionFailed("cannot resolve " + DebugString(), RequestId);
        return;
    }
    auto& entity = request->ResultSet.front();
    if (entity.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        Controller->OnDescriptionSuccess(TTableInfo(std::move(entity)), RequestId);
    } else {
        Controller->OnDescriptionFailed("incorrect path status: " + ::ToString(entity.Status), RequestId);
    }
}

void TSchemeDescriptionActorImpl::Bootstrap() {
    Become(&TSchemeDescriptionActor::StateMain);

    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = NKikimr::CanonizePath(AppData()->TenantName);
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
    InitEntry(entry);
    Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
}

NKikimrServices::TActivity::EType TSchemeDescriptionActorImpl::ActorActivityType() {
    return NKikimrServices::TActivity::METADATA_SCHEME_DESCRIPTION_ACTOR;
}

std::vector<TSysTables::TTableColumnInfo> TTableInfo::GetPKFields() const {
    std::vector<TString> result;
    std::vector<TSysTables::TTableColumnInfo> orderedKeys;
    for (auto&& i : Entry.Columns) {
        if (i.second.KeyOrder != -1) {
            orderedKeys.emplace_back(i.second);
        }
    }
    const auto pred = [](const TSysTables::TTableColumnInfo& l, const TSysTables::TTableColumnInfo& r) {
        return l.KeyOrder < r.KeyOrder;
    };
    std::sort(orderedKeys.begin(), orderedKeys.end(), pred);
    return orderedKeys;
}

std::vector<TString> TTableInfo::GetPKFieldNames() const {
    std::vector<TSysTables::TTableColumnInfo> orderedKeys = GetPKFields();
    std::vector<TString> result;
    result.reserve(orderedKeys.size());
    for (auto&& i : orderedKeys) {
        result.emplace_back(i.Name);
    }
    return result;
}

}
