#include "db_counters.h"

#include <ydb/core/base/path.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NSysView {

class TDbWatcherActor
    : public TActor<TDbWatcherActor>
{
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

    TIntrusivePtr<TDbWatcherCallback> Callback;

    THashMap<TString, TPathId> DatabaseToPathId;
    THashMap<TPathId, TString> PathIdToDatabase;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DB_WATCHER_ACTOR;
    }

    explicit TDbWatcherActor(TIntrusivePtr<TDbWatcherCallback> callback)
        : TActor(&TThis::StateFunc)
        , Callback(callback)
    {
    }

private:
    void Handle(NSysView::TEvSysView::TEvWatchDatabase::TPtr& ev) {
        auto database = ev->Get()->Database;
        auto pathId = ev->Get()->PathId;

        if (!database) {
            if (!pathId || PathIdToDatabase.FindPtr(pathId)) {
                return;
            }
            PathIdToDatabase.emplace(pathId, "");

            auto key = pathId.Hash();
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId, key));
            return;
        }

        if (DatabaseToPathId.FindPtr(database)) {
            return;
        }
        DatabaseToPathId.emplace(database, TPathId());

        auto request = MakeHolder<TNavigate>();
        request->ResultSet.push_back({});

        auto& entry = request->ResultSet.back();
        entry.Path = SplitPath(database);
        entry.Operation = TNavigate::EOp::OpPath;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        entry.RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        auto* request = ev->Get()->Request.Get();
        if (request->ResultSet.size() != 1) {
            return;
        }
        auto& entry = request->ResultSet.back();
        auto database = CanonizePath(entry.Path);
        if (entry.Status != TNavigate::EStatus::Ok) {
            DatabaseToPathId.erase(database);
            Callback->OnDatabaseRemoved(database, TPathId());
            return;
        }
        auto pathId = entry.DomainInfo->DomainKey;
        DatabaseToPathId[database] = pathId;
        PathIdToDatabase[pathId] = database;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchPathId(pathId));
    }

    void Handle(TEvTxProxySchemeCache::TEvWatchNotifyDeleted::TPtr& ev) {
        auto pathId = ev->Get()->PathId;

        if (auto* db = PathIdToDatabase.FindPtr(pathId)) {
            auto database = *db;
            if (database) {
                DatabaseToPathId.erase(database);
            }
            PathIdToDatabase.erase(pathId);

            Callback->OnDatabaseRemoved(database, pathId);

            auto key = pathId.Hash();
            Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvWatchRemove(key));
        }
    }

    STRICT_STFUNC(StateFunc, {
        hFunc(NSysView::TEvSysView::TEvWatchDatabase, Handle)
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUpdated);
        IgnoreFunc(TEvTxProxySchemeCache::TEvWatchNotifyUnavailable);
        hFunc(TEvTxProxySchemeCache::TEvWatchNotifyDeleted, Handle);
    })
};

NActors::IActor* CreateDbWatcherActor(TIntrusivePtr<TDbWatcherCallback> callback) {
    return new TDbWatcherActor(callback);
}

}
}

