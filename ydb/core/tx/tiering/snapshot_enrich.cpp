#include "snapshot_enrich.h"
#include "snapshot.h"

#include <ydb/core/base/path.h>

#include <util/string/join.h>

namespace NKikimr::NColumnShard::NTiers {

void TActorSnapshotEnrich::Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
    auto current = std::dynamic_pointer_cast<TConfigsSnapshot>(Original);
    Y_VERIFY(current);
    auto g = PassAwayGuard();

    auto* info = ev->Get();
    const auto& request = info->Request;

    if (request->ResultSet.empty()) {
        Controller->EnrichProblem("cannot resolve table path to ids");
        return;
    }

    for (auto&& i : request->ResultSet) {
        const TString path = "/" + JoinSeq("/", i.Path);
        switch (i.Status) {
            case TNavigate::EStatus::Ok:
                Cache->MutableRemapper().emplace(path, i.TableId.PathId.LocalPathId);
                current->RemapTablePathToId(path, i.TableId.PathId.LocalPathId);
                break;
            default:
                Controller->EnrichProblem(::ToString(i.Status) + " for '" + path + "'");
                return;
        }
    }
    for (auto&& i : current->GetTableTierings()) {
        Y_VERIFY(i.second.GetTablePathId());
    }
    Controller->Enriched(Original);
}

void TActorSnapshotEnrich::Bootstrap() {
    Become(&TThis::StateMain);
    auto current = std::dynamic_pointer_cast<TConfigsSnapshot>(Original);
    Y_VERIFY(current);
    const auto& remapper = Cache->GetRemapper();
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = NKikimr::CanonizePath(AppData()->TenantName);
    for (auto&& i : current->GetTableTierings()) {
        auto it = remapper.find(i.second.GetTablePath());
        if (it != remapper.end()) {
            current->RemapTablePathToId(it->first, it->second);
        } else {
            auto& entry = request->ResultSet.emplace_back();
            entry.Operation = TNavigate::OpTable;
            entry.Path = NKikimr::SplitPath(i.second.GetTablePath());
        }
    }
    if (request->ResultSet.empty()) {
        Controller->Enriched(Original);
    } else {
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }
}

}
