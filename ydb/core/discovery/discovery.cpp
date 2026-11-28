#include "discovery.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/digest/md5/md5.h>

#include <util/generic/algorithm.h>
#include <util/generic/xrange.h>
#include <util/random/shuffle.h>

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

    bool CheckEndpointId(const TString& endpointId, const NKikimrStateStorage::TEndpointBoardEntry &entry) {
        if (endpointId.empty() && !entry.HasEndpointId())
            return true;

        if (entry.HasEndpointId() && entry.GetEndpointId() == endpointId)
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
            xres->set_bridge_pile_name(entry.GetBridgePileName());
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

    void AddPileState(Ydb::Discovery::ListEndpointsResult& result, const TBridgeInfo::TPile& pile) {
        auto* pileInfo = result.add_pile_states();
        pileInfo->set_pile_name(pile.Name);
        if (pile.IsPrimary) {
            pileInfo->set_state(Ydb::Bridge::PileState::PRIMARY);
        } else if (pile.IsBeingPromoted) {
            pileInfo->set_state(Ydb::Bridge::PileState::PROMOTED);
        } else {
            switch (pile.State) {
                case NKikimrBridge::TClusterState::SYNCHRONIZED:
                    pileInfo->set_state(Ydb::Bridge::PileState::SYNCHRONIZED);
                    break;
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_1:
                case NKikimrBridge::TClusterState::NOT_SYNCHRONIZED_2:
                    pileInfo->set_state(Ydb::Bridge::PileState::NOT_SYNCHRONIZED);
                    break;
                case NKikimrBridge::TClusterState::DISCONNECTED:
                    pileInfo->set_state(Ydb::Bridge::PileState::DISCONNECTED);
                    break;
                case NKikimrBridge::TClusterState::SUSPENDED:
                    pileInfo->set_state(Ydb::Bridge::PileState::SUSPENDED);
                    break;
                case NKikimrBridge::TClusterState::HOT_STANDBY:
                    pileInfo->set_state(Ydb::Bridge::PileState::HOT_STANDBY);
                    break;
                case NKikimrBridge::TClusterState::INACTIVE:
                    pileInfo->set_state(Ydb::Bridge::PileState::INACTIVE);
                    break;
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBridge::TClusterState_EPileState_TClusterState_EPileState_INT_MAX_SENTINEL_DO_NOT_USE_:
                    Y_ABORT("Unknown pile state");
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

    bool TCachedMessageData::UpdateEntries(TMap<TActorId, TEvStateStorage::TBoardInfoEntry>&& newInfoEntries) {
        bool changed = false;
        for (auto& [actorId, info] : newInfoEntries) {
            if (info.Dropped) {
                changed |= InfoEntries.erase(actorId) != 0;
            } else {
                auto [it, added] = InfoEntries.try_emplace(actorId);
                changed |= added || it->second.Payload != info.Payload;
                it->second.Payload = std::move(info.Payload);
            }
        }
        return changed;
    }

    void TCachedMessageData::UpdateCache(const TMap<TActorId, TEvStateStorage::TBoardInfoEntry>& infoEntries,
                                         const THolder<TEvInterconnect::TEvNodeInfo>& nameserviceResponse,
                                         const TBridgeInfo::TPtr& bridgeInfo,
                                         const TString& endpointId,
                                         const TSet<TString>& services, bool longPolling) {
        TStackVec<const TString*> entries;
        entries.reserve(infoEntries.size());
        for (const auto& xpair : infoEntries) {
            entries.emplace_back(&xpair.second.Payload);
        }
        if (longPolling) {
            // Merge records for the same endpoint in a stable order, independently of their board owners.
            Sort(entries, [](const TString* a, const TString* b) { return *a < *b; });
        } else {
            Shuffle(entries.begin(), entries.end());
        }

        Ydb::Discovery::ListEndpointsResult cachedMessage;
        cachedMessage.mutable_endpoints()->Reserve(infoEntries.size());
        cachedMessage.mutable_pile_states()->Reserve(BridgeInfo ? BridgeInfo->Piles.size() : 0);

        Ydb::Discovery::ListEndpointsResult cachedMessageSsl;
        cachedMessageSsl.mutable_endpoints()->Reserve(infoEntries.size());
        cachedMessageSsl.mutable_pile_states()->Reserve(BridgeInfo ? BridgeInfo->Piles.size() : 0);

        THashMap<TEndpointKey, TEndpointState> states;
        THashMap<TEndpointKey, TEndpointState> statesSsl;

        NKikimrStateStorage::TEndpointBoardEntry entry;
        for (const TString *xpayload : entries) {
            Y_PROTOBUF_SUPPRESS_NODISCARD entry.ParseFromString(*xpayload);
            if (!CheckServices(services, entry)) {
                continue;
            }

            if (!CheckEndpointId(endpointId, entry)) {
                continue;
            }
            if (const auto implicitPileId = NBridge::GetImplicitPileId(bridgeInfo.get());
                    implicitPileId && entry.GetBridgePileName().empty()) {
                // single-pile cluster (converted from non-bridged one) may contain nodes without pile name
                entry.SetBridgePileName(bridgeInfo->GetPile(implicitPileId)->Name);
            }
            if (bridgeInfo) {
                const auto *pile = bridgeInfo->ResolvePile(entry.GetBridgePileName());
                if (pile && !NBridge::PileStateTraits(pile->State).RequiresConfigQuorum) {
                    // Match TGRpcServersManager: these piles don't accept client requests, even if their board
                    // publishers remain connected (as in HOT_STANDBY).
                    continue;
                }
            }
            if (entry.GetSsl()) {
                AddEndpoint(cachedMessageSsl, statesSsl, entry);
            } else {
                AddEndpoint(cachedMessage, states, entry);
            }
        }

        const auto &nodeInfo = nameserviceResponse->Node;
        if (nodeInfo && nodeInfo->Location.GetDataCenterId()) {
            const auto &location = nodeInfo->Location.GetDataCenterId();
            if (IsSafeLocationMarker(location)) {
                cachedMessage.set_self_location(location);
                cachedMessageSsl.set_self_location(location);
            }
        }

        if (bridgeInfo) {
            for (const auto& pile : bridgeInfo->Piles) {
                if (pile.State == NKikimrBridge::TClusterState::INACTIVE) {
                    continue; // the pile is not part of the cluster, it has no endpoints for clients
                }
                AddPileState(cachedMessage, pile);
                AddPileState(cachedMessageSsl, pile);
            }
        }

        if (longPolling) {
            auto stamp = [](auto& result) {
                auto* endpoints = result.mutable_endpoints();
                Sort(*endpoints, [](const auto& a, const auto& b) {
                    return a.address() == b.address() ? a.port() < b.port() : a.address() < b.address();
                });
                for (auto& ep : *endpoints) {
                    Sort(*ep.mutable_service());
                    Sort(*ep.mutable_ip_v4());
                    Sort(*ep.mutable_ip_v6());
                }
                SortBy(*result.mutable_pile_states(), [](const auto& pile) -> const auto& { return pile.pile_name(); });
                auto hash = MD5::CalcRaw(result.SerializeAsString());
                result.set_snapshot_hash(hash);
                Shuffle(endpoints->pointer_begin(), endpoints->pointer_end());
                return hash;
            };
            SnapshotHash = stamp(cachedMessage);
            SnapshotHashSsl = stamp(cachedMessageSsl);
        }
        CachedMessage = SerializeResult(cachedMessage);
        CachedMessageSsl = SerializeResult(cachedMessageSsl);
        BridgeInfo = bridgeInfo;
    }

    TEvDiscovery::TEvDiscoveryData* TCachedMessageData::ToEvent(bool returnSerializedMessage, bool allowEmpty) const {
        auto event = std::make_unique<TEvDiscovery::TEvDiscoveryData>();
        if (returnSerializedMessage && !CachedMessage.empty() && !CachedMessageSsl.empty()) {
            event->CachedMessage = CachedMessage;
            event->CachedMessageSsl = CachedMessageSsl;
        } else {
            event->InfoEntries = InfoEntries;
            event->BridgeInfo = BridgeInfo;
        }

        if (InfoEntries.empty() && !allowEmpty) {
            event->Status = TEvStateStorage::TEvBoardInfo::EStatus::Unknown;
        } else {
            event->Status = Status;
        }
        return event.release();
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
            bool ReturnSerializedMessage = true;
            std::shared_ptr<const NDiscovery::TListEndpointsOptions> Options;

            TEvRequest(const TString& db, bool returnSerializedMessage, std::shared_ptr<const NDiscovery::TListEndpointsOptions> options = {})
                : Database(db)
                , ReturnSerializedMessage(returnSerializedMessage)
                , Options(std::move(options))
            {
            }
        };
    };

    class TDiscoveryCache: public TActorBootstrapped<TDiscoveryCache> {
        const bool LongPolling;
        THashMap<TString, std::shared_ptr<NDiscovery::TCachedMessageData>> CurrentCachedMessages;
        THashMap<TString, std::shared_ptr<NDiscovery::TCachedMessageData>> DirtyCachedMessages;

        THashMap<TString, std::shared_ptr<NDiscovery::TCachedMessageData>> OldCachedMessages; // when subscriptions are disabled
        THashMap<TString, std::shared_ptr<NDiscovery::TCachedMessageData>> CachedNotAvailable; // for subscriptions

        THolder<TEvInterconnect::TEvNodeInfo> NameserviceResponse;
        TBridgeInfo::TPtr BridgeInfo;

        struct TWaiter {
            TActorId ActorId;
            ui64 Cookie;
            bool ReturnSerializedMessage;
            std::shared_ptr<const NDiscovery::TListEndpointsOptions> Options;
        };

        THashMap<TString, THashMap<TActorId, TWaiter>> Waiters;

        TMaybe<TString> EndpointId;
        TMaybe<TActorId> NameserviceActorId;

        THashMap<TString, TMonotonic> InFlightLookups; // board path to lookup start time
        ui64 LastCookie = 0;

        NMonitoring::TDynamicCounterPtr Counters;

        NMonitoring::THistogramPtr BoardLookupLatency;

        bool UseBoardSubscriptions() const {
            return LongPolling || AppData()->FeatureFlags.GetEnableSubscriptionsInDiscovery();
        }

        void Request(const TString& database) {
            if (CurrentStateFunc() == &TThis::StateWork && InFlightLookups.emplace(database, TMonotonic::Now()).second) {
                if (!LastCookie) {
                    Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
                }
                const auto mode = UseBoardSubscriptions() ? EBoardLookupMode::Subscription : EBoardLookupMode::Second;
                auto cookie = ++LastCookie;
                YDB_LOG_DEBUG_COMP(NKikimrServices::DISCOVERY_CACHE, "Lookup",
                    {"path", database},
                    {"cookie", cookie});
                Register(CreateBoardLookupActor(database, SelfId(), mode, {}, cookie));
            }
        }

        bool ReplyIfChanged(const TString& path, NDiscovery::TCachedMessageData& data, const TWaiter& waiter) {
            if ((LongPolling && DirtyCachedMessages.erase(path)) || data.BridgeInfo != BridgeInfo) {
                RefreshSnapshot(data);
            }
            const auto& options = waiter.Options;
            const auto* view = &data;
            TMaybe<NDiscovery::TCachedMessageData> filtered;
            if (options && (!options->EndpointId.empty() || !options->Services.empty())) {
                filtered.ConstructInPlace(TString{}, TString{}, decltype(data.InfoEntries){}, data.BridgeInfo, data.Status);
                filtered->UpdateCache(data.InfoEntries, NameserviceResponse, data.BridgeInfo, options->EndpointId, options->Services, LongPolling);
                view = &*filtered;
            }
            const bool waiting = LongPolling && options && !options->SnapshotHash.empty();
            if (waiting && data.Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok &&
                options->SnapshotHash == (options->Ssl ? view->SnapshotHashSsl : view->SnapshotHash)) {
                return false;
            }
            Send(waiter.ActorId, view->ToEvent(waiter.ReturnSerializedMessage, waiting || !data.InfoEntries.empty()), 0, waiter.Cookie);
            return true;
        }

        void Handle(TEvDiscovery::TEvCancelPoll::TPtr& ev) {
            const auto& path = ev->Get()->Path;
            auto it = Waiters.find(path);
            if (it != Waiters.end() && it->second.erase(ev->Sender) && it->second.empty()) {
                Waiters.erase(it);
            }
        }

        void NotifyWaiters(const TString& path, NDiscovery::TCachedMessageData& data) {
            if (auto it = Waiters.find(path); it != Waiters.end()) {
                EraseNodesIf(it->second, [&](const auto& item) { return ReplyIfChanged(path, data, item.second); });
                if (it->second.empty()) {
                    Waiters.erase(it);
                }
            }
        }

        void RefreshSnapshot(NDiscovery::TCachedMessageData& data) {
            data.UpdateCache(data.InfoEntries, NameserviceResponse, BridgeInfo, EndpointId.GetOrElse({}), {}, LongPolling);
        }

        void Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev) {
            NameserviceResponse.Reset(ev->Release().Release());
            TryFinishInitialization();
        }

        void Handle(TEvStateStorage::TEvBoardInfoUpdate::TPtr ev) {
            YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY_CACHE, "Handle",
                {"ev", ev->Get()->ToString()});
            if (!UseBoardSubscriptions()) {
                return;
            }
            THolder<TEvStateStorage::TEvBoardInfoUpdate> msg = ev->Release();
            const auto& path = msg->Path;

            if (msg->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
                DirtyCachedMessages.erase(path);
                if (auto* current = CurrentCachedMessages.FindPtr(path)) {
                    (*current)->Status = msg->Status;
                    NotifyWaiters(path, **current);
                    CurrentCachedMessages.erase(path);
                }
                return;
            }

            auto* current = CurrentCachedMessages.FindPtr(path);
            if (!current) {
                return;
            }
            auto& currentCachedMessage = *current;

            if (currentCachedMessage->UpdateEntries(std::move(msg->Updates))) {
                DirtyCachedMessages[path] = currentCachedMessage;
                NotifyWaiters(path, *currentCachedMessage);
            }

            Y_ABORT_UNLESS(!InFlightLookups.contains(path));
        }

        void Handle(TEvStateStorage::TEvBoardInfo::TPtr ev) {
            YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY_CACHE, "Handle",
                {"ev", ev->Get()->ToString()});

            THolder<TEvStateStorage::TEvBoardInfo> msg = ev->Release();

            const auto& path = msg->Path;
            if (auto it = InFlightLookups.find(path); it != InFlightLookups.end()) {
                auto duration = TMonotonic::Now() - it->second;
                InFlightLookups.erase(it);

                BoardLookupLatency->Collect(duration.MicroSeconds());
            }

            Y_ABORT_UNLESS(NameserviceResponse);
            Y_ABORT_UNLESS(!IsBridgeMode(ActorContext()) || BridgeInfo);

            auto newCachedData = std::make_shared<NDiscovery::TCachedMessageData>(std::move(msg->InfoEntries),
                NameserviceResponse, BridgeInfo, EndpointId.GetOrElse({}), TSet<TString>{}, msg->Status, LongPolling);

            DirtyCachedMessages.erase(path);

            if (UseBoardSubscriptions()) {
                if (msg->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
                    CurrentCachedMessages.erase(path);
                    CachedNotAvailable[path] = newCachedData;
                } else {
                    CurrentCachedMessages[path] = newCachedData;
                }
            } else {
                CurrentCachedMessages[path] = newCachedData;
                OldCachedMessages.erase(path);
            }

            NotifyWaiters(path, *newCachedData);
        }

        void Wakeup() {
            if (UseBoardSubscriptions()) {
                CachedNotAvailable.clear();
            } else {
                OldCachedMessages.swap(CurrentCachedMessages);
                CurrentCachedMessages.clear();
            }

            Y_ABORT_UNLESS(NameserviceResponse);
            Y_ABORT_UNLESS(!IsBridgeMode(ActorContext()) || BridgeInfo);

            for (auto& [_, cachedData] : DirtyCachedMessages) {
                RefreshSnapshot(*cachedData);
            }

            DirtyCachedMessages.clear();

            Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
        }

        void Handle(TEvPrivate::TEvRequest::TPtr& ev) {
            YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY_CACHE, "Handle",
                {"ev", ev->ToString()});

            const auto* msg = ev->Get();
            TWaiter waiter{ev->Sender, ev->Cookie, msg->ReturnSerializedMessage, msg->Options};

            const auto* cachedData = CurrentCachedMessages.FindPtr(msg->Database);
            if (!cachedData) {
                const bool subscribed = UseBoardSubscriptions();
                cachedData = (subscribed ? CachedNotAvailable : OldCachedMessages).FindPtr(msg->Database);
                if (!cachedData || !subscribed) {
                    Request(msg->Database);
                }
            }

            if (!cachedData || !ReplyIfChanged(msg->Database, **cachedData, waiter)) {
                Waiters[msg->Database][ev->Sender] = std::move(waiter);
            }
        }

        void HandleOnInitialization(TEvNodeWardenStorageConfig::TPtr& ev) {
            YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY_CACHE, "Handle on initialization",
                {"ev", ev->Get()->ToString()});
            BridgeInfo = ev->Get()->BridgeInfo;
            TryFinishInitialization();
        }

        void HandleOnWork(TEvNodeWardenStorageConfig::TPtr& ev) {
            YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY_CACHE, "Handle on work",
                {"ev", ev->Get()->ToString()});
            if (BridgeInfo != ev->Get()->BridgeInfo) {
                BridgeInfo = ev->Get()->BridgeInfo;
                for (const auto& [path, data] : CurrentCachedMessages) {
                    DirtyCachedMessages[path] = data;
                    NotifyWaiters(path, *data);
                }
            }
        }

    public:
        TDiscoveryCache(const TString& endpointId, const TMaybe<TActorId>& nameserviceActorId, bool longPolling)
            : LongPolling(longPolling)
            , EndpointId(endpointId)
            , NameserviceActorId(nameserviceActorId)
        {
        }
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::DISCOVERY_CACHE_ACTOR;
        }

        void Bootstrap(const TActorContext& ctx) {
            Send(NameserviceActorId.GetOrElse(GetNameserviceActorId()), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));
            if (IsBridgeMode(ctx)) {
                const TActorId wardenId = MakeBlobStorageNodeWardenID(SelfId().NodeId());
                Send(wardenId, new TEvNodeWardenQueryStorageConfig(true));
            }

            Counters = GetServiceCounters(AppData()->Counters, "grpc")->GetSubgroup("subsystem", "discovery");

            BoardLookupLatency = Counters->GetNamedHistogram("sensor", "BoardLookupLatency_us",
                NMonitoring::ExponentialHistogram(20, 2, 1));

            Become(&TThis::Initializing);
        }

        void TryFinishInitialization() {
            if (!NameserviceResponse || (IsBridgeMode(ActorContext()) && !BridgeInfo)) {
                return;
            }

            YDB_LOG_DEBUG_COMP(NKikimrServices::DISCOVERY_CACHE, "Finish initialization",
                {"awaitingPaths", Waiters.size()});

            Become(&TThis::StateWork);

            for (const auto& [path, _] : Waiters) {
                Request(path);
            }
        }

        STATEFN(Initializing) {
            if (ev->InterconnectSession) {
                return;
            }
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDiscovery::TEvCancelPoll, Handle);
                hFunc(TEvPrivate::TEvRequest, Handle);
                hFunc(TEvInterconnect::TEvNodeInfo, Handle);
                hFunc(TEvNodeWardenStorageConfig, HandleOnInitialization);
                sFunc(TEvents::TEvPoison, PassAway);
            }
        }

        STATEFN(StateWork) {
            if (ev->InterconnectSession) {
                return;
            }
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDiscovery::TEvCancelPoll, Handle);
                hFunc(TEvPrivate::TEvRequest, Handle);
                hFunc(TEvStateStorage::TEvBoardInfo, Handle);
                hFunc(TEvStateStorage::TEvBoardInfoUpdate, Handle);
                hFunc(TEvNodeWardenStorageConfig, HandleOnWork);
                sFunc(TEvents::TEvWakeup, Wakeup);
                sFunc(TEvents::TEvPoison, PassAway);
            }
        }
    };
}

class TDiscoverer: public TActorBootstrapped<TDiscoverer> {
    TLookupPathFunc MakeLookupPath;
    const TString Database;
    const bool ReturnSerializedMessage;
    const TActorId ReplyTo;
    const TActorId CacheId;
    std::shared_ptr<const NDiscovery::TListEndpointsOptions> Options;
    const bool WaitForChange;
    TString PollPath;

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
            bool returnSerializedMessage,
            const TActorId& replyTo, const TActorId& cacheId, std::shared_ptr<const NDiscovery::TListEndpointsOptions> options)
        : MakeLookupPath(f)
        , Database(database)
        , ReturnSerializedMessage(returnSerializedMessage)
        , ReplyTo(replyTo)
        , CacheId(cacheId)
        , Options(std::move(options))
        , WaitForChange(Options && !Options->SnapshotHash.empty())
    {
    }

    void PassAway() override {
        if (!PollPath.empty()) {
            Send(CacheId, new TEvDiscovery::TEvCancelPoll(PollPath));
        }
        TActorBootstrapped<TDiscoverer>::PassAway();
    }

    void Bootstrap() {
        if (!WaitForChange) {
            Lookup(Database);
        }
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
        YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY, "Handle",
            {"ev", ev->ToString()},
            {"cookie", ev->Cookie});

        if (ev->Cookie != LookupCookie) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::DISCOVERY, "Stale lookup response",
                {"got", ev->Cookie},
                {"expected", LookupCookie});
            return;
        }

        PollPath.clear();
        LookupResponse.Reset(ev->Release().Release());

        MaybeReply();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        SchemeCacheResponse.Reset(ev->Release().Release());

        const auto* response = SchemeCacheResponse.Get()->Request.Get();

        Y_ABORT_UNLESS(response->ResultSet.size() == 1);
        const auto& entry = response->ResultSet.front();

        YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY, "Handle",
            {"ev", SchemeCacheResponse->ToString()},
            {"entry", entry.ToString()});

        if (response->ErrorCount > 0) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::DATABASE_NOT_EXIST,
                    "Requested database does not exist"));
            default:
                YDB_LOG_DEBUG_COMP(NKikimrServices::DISCOVERY, "Unexpected status",
                    {"entry", entry.ToString()});
                return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::RESOLVE_ERROR,
                    "Database resolve failed with no certain result"));
            }
        }

        if (!entry.DomainInfo) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::DISCOVERY, "Empty domain info",
                {"entry", entry.ToString()});
            return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::RESOLVE_ERROR,
                "Database resolve failed with no certain result"));
        }

        const auto isDomain = entry.Path.size() == 1;
        const auto isSubDomain = entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindSubdomain
            || entry.Kind == NSchemeCache::TSchemeCacheNavigate::KindExtSubdomain;

        if (!isDomain && !isSubDomain) {
            YDB_LOG_WARN_COMP(NKikimrServices::DISCOVERY, "Path is not database",
                {"path", CanonizePath(Database)});
            return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::ACCESS_DENIED,
                "Requested path is not database name"));
        }

        auto info = entry.DomainInfo;
        if (NeedResolveResources(info)) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::DISCOVERY, "Resolve resources domain",
                {"domainKey", info->DomainKey},
                {"resourcesDomainKey", info->ResourcesDomainKey});

            Navigate(info->ResourcesDomainKey);
            ResolveResources = true;
        } else if (ResolveResources || WaitForChange) {
            Lookup(CanonizePath(entry.Path));
        }

        MaybeReply();
    }

    static bool NeedResolveResources(TIntrusivePtr<NSchemeCache::TDomainInfo> domainInfo) {
        if (!domainInfo->IsServerless()) {
            return false;
        }

        if (domainInfo->ServerlessComputeResourcesMode.Empty()) {
            return true;
        }

        switch (*domainInfo->ServerlessComputeResourcesMode) {
            case NKikimrSubDomains::EServerlessComputeResourcesModeExclusive:
                return false;
            case NKikimrSubDomains::EServerlessComputeResourcesModeShared:
                return true;
            default:
                return true;
        }
    }

    void MaybeReply() {
        if (!LookupResponse || !SchemeCacheResponse) {
            return;
        }

        if (LookupResponse->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            YDB_LOG_DEBUG_COMP(NKikimrServices::DISCOVERY, "Lookup error",
                {"status", ui64(LookupResponse->Status)});
            return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::RESOLVE_ERROR,
                "Database nodes resolve failed with no certain result"));
        }

        Reply(LookupResponse.Release());
    }

    void Lookup(const TString& db) {
        YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY, "Lookup",
            {"path", db});

        const auto path = NKikimr::SplitPath(db);
        const auto domainName = path ? path[0] : TString();
        auto* domainInfo = AppData()->DomainsInfo->GetDomainByName(domainName);
        if (!domainInfo) {
            return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::DATABASE_NOT_EXIST,
                "Database " + domainName + " does not exist"));
        }

        TString database;
        for (const auto& token : path) {
            if (token.size() > 4100) {
                return Reply(new TEvDiscovery::TEvError(TEvDiscovery::TEvError::KEY_PARSE_ERROR,
                    "Requested database name too long"));
            }

            database.append("/").append(token);
        }

        const auto reqPath = MakeLookupPath(database);
        if (WaitForChange) {
            PollPath = reqPath;
        }
        Send(CacheId, new NDiscoveryPrivate::TEvPrivate::TEvRequest(reqPath, ReturnSerializedMessage, Options), 0, ++LookupCookie);
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
        YDB_LOG_TRACE_COMP(NKikimrServices::DISCOVERY, "Navigate",
            {"path", id});

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        request->DatabaseName = AppData()->DomainsInfo->GetDomain()->Name;

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
        bool returnSerializedMessage,
        const TActorId& replyTo,
        const TActorId& cacheId, std::shared_ptr<const NDiscovery::TListEndpointsOptions> options) {
    return new TDiscoverer(f, database, returnSerializedMessage, replyTo, cacheId, std::move(options));
}

IActor* CreateDiscoveryCache(const TString& endpointId, const TMaybe<TActorId>& nameserviceActorId, bool longPolling) {
    return new NDiscoveryPrivate::TDiscoveryCache(endpointId, nameserviceActorId, longPolling);
}

}
