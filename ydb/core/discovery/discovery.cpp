#include "discovery.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/xrange.h>
#include <util/random/shuffle.h>

#define LOG_T(service, stream) LOG_TRACE_S(*TlsActivationContext, service, stream)
#define LOG_D(service, stream) LOG_DEBUG_S(*TlsActivationContext, service, stream)

#define CLOG_T(stream) LOG_T(NKikimrServices::DISCOVERY_CACHE, stream)
#define CLOG_D(stream) LOG_D(NKikimrServices::DISCOVERY_CACHE, stream)

#define DLOG_T(stream) LOG_T(NKikimrServices::DISCOVERY, stream)
#define DLOG_D(stream) LOG_D(NKikimrServices::DISCOVERY, stream)

namespace NKikimr {

namespace NDiscovery {
    using TEndpointKey = std::pair<TString, ui32>;
    struct TEndpointState {
        int Index = -1;
        int Count = 0;
        float LoadFactor = 0;
        THashSet<TString> Locations;
        THashSet<TString> Services;
    };

    bool CheckServices(const TSet<TString> &req, const NKikimrStateStorage::TEndpointBoardEntry &entry) {
        if (req.empty())
            return true;

        for (const auto &x : entry.GetServices())
            if (req.count(x))
                return true;

        return false;
    }

    bool IsSafeLocationMarker(TStringBuf location) {
        const ui8* isrc = reinterpret_cast<const ui8*>(location.data());
        for (auto idx : xrange(location.size())) {
            if (isrc[idx] >= 0x80)
                return false;
        }
        return true;
    }

    void AddEndpoint(
            Ydb::Discovery::ListEndpointsResult& result,
            THashMap<TEndpointKey, TEndpointState>& states,
            const NKikimrStateStorage::TEndpointBoardEntry& entry) {
        Ydb::Discovery::EndpointInfo *xres;

        auto& state = states[TEndpointKey(entry.GetAddress(), entry.GetPort())];
        if (state.Index >= 0) {
            xres = result.mutable_endpoints(state.Index);
            ++state.Count;
            // FIXME: do we want a mean or a sum here?
            // xres->set_load_factor(xres->load_factor() + (entry.GetLoad() - xres->load_factor()) / state.Count);
            xres->set_load_factor(xres->load_factor() + entry.GetLoad());
        } else {
            state.Index = result.endpoints_size();
            state.Count = 1;
            xres = result.add_endpoints();
            xres->set_address(entry.GetAddress());
            xres->set_port(entry.GetPort());
            if (entry.GetSsl())
                xres->set_ssl(true);
            xres->set_load_factor(entry.GetLoad());
            xres->set_node_id(entry.GetNodeId());
            if (entry.AddressesV4Size()) {
                xres->mutable_ip_v4()->Reserve(entry.AddressesV4Size());
                for (const auto& addr : entry.GetAddressesV4()) {
                    xres->add_ip_v4(addr);
                }
            }
            if (entry.AddressesV6Size()) {
                xres->mutable_ip_v6()->Reserve(entry.AddressesV6Size());
                for (const auto& addr : entry.GetAddressesV6()) {
                    xres->add_ip_v6(addr);
                }
            }
            xres->set_ssl_target_name_override(entry.GetTargetNameOverride());
        }

        if (IsSafeLocationMarker(entry.GetDataCenter())) {
            if (state.Locations.insert(entry.GetDataCenter()).second) {
                if (xres->location().empty()) {
                    xres->set_location(entry.GetDataCenter());
                } else {
                    xres->set_location(xres->location() + "/" + entry.GetDataCenter());
                }
            }
        }

        for (auto &service : entry.GetServices()) {
            if (state.Services.insert(service).second) {
                xres->add_service(service);
            }
        }
    }

    TString SerializeResult(const Ydb::Discovery::ListEndpointsResult& result) {
        Ydb::Discovery::ListEndpointsResponse response;
        TString out;
        auto deferred = response.mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(Ydb::StatusIds::SUCCESS);

        auto data = deferred->mutable_result();
        data->PackFrom(result);

        Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);
        return out;
    }

    std::pair<TString, TString> CreateSerializedMessage(
                const THolder<TEvStateStorage::TEvBoardInfo>& info,
                TSet<TString> services,
                const THolder<TEvInterconnect::TEvNodeInfo>& nameserviceResponse) {
        TStackVec<const TString*> entries;
        entries.reserve(info->InfoEntries.size());
        for (auto &xpair : info->InfoEntries)
            entries.emplace_back(&xpair.second.Payload);
        Shuffle(entries.begin(), entries.end());

        Ydb::Discovery::ListEndpointsResult result;
        result.mutable_endpoints()->Reserve(info->InfoEntries.size());

        Ydb::Discovery::ListEndpointsResult resultSsl;
        resultSsl.mutable_endpoints()->Reserve(info->InfoEntries.size());

        THashMap<TEndpointKey, TEndpointState> states;
        THashMap<TEndpointKey, TEndpointState> statesSsl;

        NKikimrStateStorage::TEndpointBoardEntry entry;
        for (const TString *xpayload : entries) {
            Y_PROTOBUF_SUPPRESS_NODISCARD entry.ParseFromString(*xpayload);
            if (!CheckServices(services, entry)) {
                continue;
            }

            if (entry.GetSsl()) {
                AddEndpoint(resultSsl, statesSsl, entry);
            } else {
                AddEndpoint(result, states, entry);
            }
        }

        const auto &nodeInfo = nameserviceResponse->Node;
        if (nodeInfo && nodeInfo->Location.GetDataCenterId()) {
            const auto &location = nodeInfo->Location.GetDataCenterId();
            if (IsSafeLocationMarker(location)) {
                result.set_self_location(location);
                resultSsl.set_self_location(location);
            }
        }

        return {SerializeResult(result), SerializeResult(resultSsl)};
    }
}

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

    class TDiscoveryCache: public TActorBootstrapped<TDiscoveryCache> {
        THashMap<TString, std::shared_ptr<NDiscovery::TCachedMessageData>> NewCachedMessages;
        THashMap<TString, std::shared_ptr<NDiscovery::TCachedMessageData>> OldCachedMessages;
        THolder<TEvInterconnect::TEvNodeInfo> NameserviceResponse;

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

        void Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev) {
            NameserviceResponse.Reset(ev->Release().Release());
        }

        void Handle(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
            CLOG_T("Handle " << ev->Get()->ToString());

            THolder<TEvStateStorage::TEvBoardInfo> msg = ev->Release();
            const auto& path = msg->Path;

            auto newCachedData = std::make_shared<NDiscovery::TCachedMessageData>();

            if (NameserviceResponse) {
                auto result = NDiscovery::CreateSerializedMessage(msg, {}, NameserviceResponse);

                newCachedData->CachedMessage = result.first;
                newCachedData->CachedMessageSsl = result.second;
            }

            newCachedData->Info = std::move(msg);

            OldCachedMessages.erase(path);
            NewCachedMessages.emplace(path, newCachedData);

            if (auto it = Requested.find(path); it != Requested.end()) {
                for (const auto& waiter : it->second) {
                    Send(waiter.ActorId,
                        new TEvDiscovery::TEvDiscoveryData(newCachedData), 0, waiter.Cookie);
                }
                Requested.erase(it);
            }

            if (!Scheduled) {
                Scheduled = true;
                Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
            }
        }

        void Wakeup() {
            OldCachedMessages.swap(NewCachedMessages);
            NewCachedMessages.clear();

            if (!OldCachedMessages.empty()) {
                Scheduled = true;
                Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
            } else {
                Scheduled = false;
            }
        }

        void Handle(TEvPrivate::TEvRequest::TPtr& ev) {
            CLOG_T("Handle " << ev->Get()->ToString());

            const auto* msg = ev->Get();

            const auto* cachedData = NewCachedMessages.FindPtr(msg->Database);
            if (cachedData == nullptr) {
                cachedData = OldCachedMessages.FindPtr(msg->Database);
                if (cachedData == nullptr) {
                    Request(msg->Database, msg->StateStorageId, {ev->Sender, ev->Cookie});
                    return;
                }
                Request(msg->Database, msg->StateStorageId);
            }

            Send(ev->Sender, new TEvDiscovery::TEvDiscoveryData(*cachedData), 0, ev->Cookie);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::DISCOVERY_CACHE_ACTOR;
        }

        void Bootstrap() {
            Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));

            Become(&TThis::StateWork);
        }

        STATEFN(StateWork) {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvRequest, Handle);
                hFunc(TEvStateStorage::TEvBoardInfo, Handle);
                hFunc(TEvInterconnect::TEvNodeInfo, Handle);
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

    THolder<TEvDiscovery::TEvDiscoveryData> LookupResponse;
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

    explicit TDiscoverer(
            TLookupPathFunc f, const TString& database,
            const TActorId& replyTo, const TActorId& cacheId)
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
            hFunc(TEvDiscovery::TEvDiscoveryData, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            sFunc(TEvents::TEvPoison, PassAway);
        }
    }

    void Handle(TEvDiscovery::TEvDiscoveryData::TPtr& ev) {
        Y_VERIFY(ev->Get()->CachedMessageData);

        if (ev->Get()->CachedMessageData->Info) {
            DLOG_T("Handle " << ev->Get()->CachedMessageData->Info->ToString()
                << ": cookie# " << ev->Cookie);
        }

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

        if (LookupResponse->CachedMessageData->Info &&
                LookupResponse->CachedMessageData->Info->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            DLOG_D("Lookup error"
                << ": status# " << ui64(LookupResponse->CachedMessageData->Info->Status));
            return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::RESOLVE_ERROR,
                "Database nodes resolve failed with no certain result"));
        }

        Reply(LookupResponse.Release());
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

IActor* CreateDiscoverer(
        TLookupPathFunc f,
        const TString& database,
        const TActorId& replyTo,
        const TActorId& cacheId) {
    return new TDiscoverer(f, database, replyTo, cacheId);
}

IActor* CreateDiscoveryCache() {
    return new NDiscoveryPrivate::TDiscoveryCache();
}

}
