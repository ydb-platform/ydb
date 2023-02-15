#include "discovery.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#define LOG_T(service, stream) LOG_TRACE_S(*TlsActivationContext, service, stream)
#define LOG_D(service, stream) LOG_DEBUG_S(*TlsActivationContext, service, stream)

#define CLOG_T(stream) LOG_T(NKikimrServices::DISCOVERY_CACHE, stream)
#define CLOG_D(stream) LOG_D(NKikimrServices::DISCOVERY_CACHE, stream)

#define DLOG_T(stream) LOG_T(NKikimrServices::DISCOVERY, stream)
#define DLOG_D(stream) LOG_D(NKikimrServices::DISCOVERY, stream)

namespace NKikimr {

namespace NDiscoveryPrivate {
    struct TEvPrivate {
        enum EEv {
            EvRequest = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvEnd
        };

        struct TEvRequest: public TEventLocal<TEvRequest, EvRequest> {
            const TString Database;
            const ui32 StateStorageId;

            TEvRequest(const TString& db, ui32 stateStorageId)
                : Database(db)
                , StateStorageId(stateStorageId)
            {
            }
        };
    };

    class TDiscoveryCache: public TActor<TDiscoveryCache> {
        THashMap<TString, THolder<TEvStateStorage::TEvBoardInfo>> OldInfo;
        THashMap<TString, THolder<TEvStateStorage::TEvBoardInfo>> NewInfo;

        struct TWaiter {
            TActorId ActorId;
            ui64 Cookie;
        };

        THashMap<TString, TVector<TWaiter>> Requested;
        bool Scheduled = false;

        auto Request(const TString& database, ui32 groupId) {
            auto result = Requested.emplace(database, TVector<TWaiter>());
            if (result.second) {
                CLOG_D("Lookup"
                    << ": path# " << database);
                Register(CreateBoardLookupActor(database, SelfId(), groupId, EBoardLookupMode::Second, false, false));
            }

            return result.first;
        }

        void Request(const TString& database, ui32 groupId, const TWaiter& waiter) {
            auto it = Request(database, groupId);
            it->second.push_back(waiter);
        }

        void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
            CLOG_T("Handle " << ev->Get()->ToString());

            THolder<TEvStateStorage::TEvBoardInfo> msg = ev->Release();
            const auto& path = msg->Path;

            if (auto it = Requested.find(path); it != Requested.end()) {
                for (const auto& waiter : it->second) {
                    Send(waiter.ActorId, new TEvStateStorage::TEvBoardInfo(*msg), 0, waiter.Cookie);
                }

                Requested.erase(it);
            }

            OldInfo.erase(path);
            NewInfo.emplace(path, std::move(msg));

            if (!Scheduled) {
                Scheduled = true;
                Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
            }
        }

        void Wakeup() {
            OldInfo.swap(NewInfo);
            NewInfo.clear();

            if (!OldInfo.empty()) {
                Scheduled = true;
                Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
            } else {
                Scheduled = false;
            }
        }

        void Handle(TEvPrivate::TEvRequest::TPtr& ev) {
            CLOG_T("Handle " << ev->Get()->ToString());

            const auto* msg = ev->Get();

            if (const auto* x = NewInfo.FindPtr(msg->Database)) {
                Send(ev->Sender, new TEvStateStorage::TEvBoardInfo(**x), 0, ev->Cookie);
                return;
            }

            if (const auto* x = OldInfo.FindPtr(msg->Database)) {
                Request(msg->Database, msg->StateStorageId);
                Send(ev->Sender, new TEvStateStorage::TEvBoardInfo(**x), 0, ev->Cookie);
                return;
            }

            Request(msg->Database, msg->StateStorageId, {ev->Sender, ev->Cookie});
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::DISCOVERY_CACHE_ACTOR;
        }

        TDiscoveryCache()
            : TActor(&TThis::StateWork)
        {
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvRequest, Handle);
                hFunc(TEvStateStorage::TEvBoardInfo, Handle);
                sFunc(TEvents::TEvWakeup, Wakeup);
                sFunc(TEvents::TEvPoison, PassAway);
            }
        }
    };
}

class TDiscoverer: public TActorBootstrapped<TDiscoverer> {
    TLookupPathFunc MakeLookupPath;
    const TString Database;
    const TActorId ReplyTo;
    const TActorId CacheId;

    THolder<TEvStateStorage::TEvBoardInfo> LookupResponse;
    THolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult> SchemeCacheResponse;

    bool ResolveResources = false;
    ui64 LookupCookie = 0;

    void Reply(IEventBase* ev) {
        Send(ReplyTo, ev);
        PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DISCOVERY_ACTOR;
    }

    explicit TDiscoverer(TLookupPathFunc f, const TString& database, const TActorId& replyTo, const TActorId& cacheId)
        : MakeLookupPath(f)
        , Database(database)
        , ReplyTo(replyTo)
        , CacheId(cacheId)
    {
    }

    void Bootstrap() {
        Lookup(Database);
        Navigate(Database);

        Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        DLOG_T("Handle " << LookupResponse->ToString()
            << ": cookie# " << ev->Cookie);

        if (ev->Cookie != LookupCookie) {
            DLOG_D("Stale lookup response"
                << ": got# " << ev->Cookie
                << ", expected# " << LookupCookie);
            return;
        }

        LookupResponse.Reset(ev->Release().Release());

        MaybeReply();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        SchemeCacheResponse.Reset(ev->Release().Release());

        const auto* response = SchemeCacheResponse.Get()->Request.Get();

        Y_VERIFY(response->ResultSet.size() == 1);
        const auto& entry = response->ResultSet.front();

        DLOG_T("Handle " << SchemeCacheResponse->ToString()
            << ": entry# " << entry.ToString());

        if (response->ErrorCount > 0) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::DATABASE_NOT_EXIST,
                    "Requested database not exists"));
            default:
                DLOG_D("Unexpected status"
                    << ": entry# " << entry.ToString());
                return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::RESOLVE_ERROR,
                    "Database resolve failed with no certain result"));
            }
        }

        if (!entry.DomainInfo) {
            DLOG_D("Empty domain info"
                << ": entry# " << entry.ToString());
            return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::RESOLVE_ERROR,
                "Database resolve failed with no certain result"));
        }

        auto info = entry.DomainInfo;
        if (info->DomainKey != info->ResourcesDomainKey) {
            DLOG_D("Resolve resources domain"
                << ": domain key# " << info->DomainKey
                << ", resources domain key# " << info->ResourcesDomainKey);

            Navigate(info->ResourcesDomainKey);
            ResolveResources = true;
        } else if (ResolveResources) {
            Lookup(CanonizePath(entry.Path));
        }

        MaybeReply();
    }

    void MaybeReply() {
        if (!LookupResponse || !SchemeCacheResponse) {
            return;
        }

        {
            // check presence of database (acl should be checked here too)
            const auto& entry = SchemeCacheResponse->Request->ResultSet.front();
            const auto isDomain = entry.Path.size() == 1;
            const auto isSubDomain = entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindSubdomain
                || entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindExtSubdomain;

            if (!isDomain && !isSubDomain) {
                DLOG_D("Path is not database"
                    << ": entry# " << entry.ToString());
                return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::ACCESS_DENIED,
                    "Requested path is not database name"));
            }
        }

        if (LookupResponse->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            DLOG_D("Lookup error"
                << ": status# " << ui64(LookupResponse->Status));
            return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::RESOLVE_ERROR,
                "Database nodes resolve failed with no certain result"));
        }

        Reply(new TEvStateStorage::TEvBoardInfo(*LookupResponse));
    }

    void Lookup(const TString& db) {
        DLOG_T("Lookup"
            << ": path# " << db);

        const auto path = NKikimr::SplitPath(db);
        const auto domainName = path ? path[0] : TString();
        auto* domainInfo = AppData()->DomainsInfo->GetDomainByName(domainName);
        if (!domainInfo) {
            return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::DATABASE_NOT_EXIST,
                "Database " + domainName + " not exists"));
        }

        TString database;
        for (const auto& token : path) {
            if (token.size() > 4100) {
                return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::KEY_PARSE_ERROR,
                    "Requested database name too long"));
            }

            database.append("/").append(token);
        }

        const auto stateStorageGroupId = domainInfo->DefaultStateStorageGroup;
        const auto reqPath = MakeLookupPath(database);

        Send(CacheId, new NDiscoveryPrivate::TEvPrivate::TEvRequest(reqPath, stateStorageGroupId), 0, ++LookupCookie);
        LookupResponse.Reset();
    }

    static void FillNavigateKey(const TString& path, NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        entry.Path = NKikimr::SplitPath(path);
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
    }

    static void FillNavigateKey(const TPathId& pathId, NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        entry.TableId = TTableId(pathId.OwnerId, pathId.LocalPathId);
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    }

    template <typename T>
    void Navigate(const T& id) {
        DLOG_T("Navigate"
            << ": path# " << id);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        entry.RedirectRequired = false;
        FillNavigateKey(id, entry);

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()));
        SchemeCacheResponse.Reset();
    }
};

IActor* CreateDiscoverer(TLookupPathFunc f, const TString& database, const TActorId& replyTo, const TActorId& cacheId) {
    return new TDiscoverer(f, database, replyTo, cacheId);
}

IActor* CreateDiscoveryCache() {
    return new NDiscoveryPrivate::TDiscoveryCache();
}

}
