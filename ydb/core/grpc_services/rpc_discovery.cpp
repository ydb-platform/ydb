#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_kqp_base.h"

#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/location.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/random/shuffle.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

namespace NDiscoveryPrivate {
    struct TEvPrivate {
        enum EEv {
            EvRequest = EventSpaceBegin(TKikimrEvents::ES_PRIVATE),
            EvEnd
        };

        struct TEvRequest : public TEventLocal<TEvRequest, EvRequest> {
            const TString Database;
            const ui32 StateStorageId;

            TEvRequest(const TString &db, ui32 stateStorageId)
                : Database(db)
                , StateStorageId(stateStorageId)
            {}
        };
    };

    class TDiscoveryCache : public TActor<TDiscoveryCache> {
        THashMap<TString, THolder<TEvStateStorage::TEvBoardInfo>> OldInfo;
        THashMap<TString, THolder<TEvStateStorage::TEvBoardInfo>> NewInfo;

        struct TWaiter {
            TActorId ActorId;
            ui64 Cookie;
        };

        THashMap<TString, TVector<TWaiter>> Requested;
        bool Scheduled;

        void Handle(TEvStateStorage::TEvBoardInfo::TPtr &ev) {
            THolder<TEvStateStorage::TEvBoardInfo> msg = ev->Release();
            const TString &path = msg->Path;

            auto vecIt = Requested.find(path);
            if (vecIt != Requested.end()) {
                for (auto &x : vecIt->second)
                    Send(x.ActorId, new TEvStateStorage::TEvBoardInfo(*msg), 0, x.Cookie);
                Requested.erase(vecIt);
            }

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

        void Handle(TEvPrivate::TEvRequest::TPtr &ev) {
            auto *msg = ev->Get();
            if (auto *x = OldInfo.FindPtr(msg->Database)) {
                Send(ev->Sender, new TEvStateStorage::TEvBoardInfo(**x), 0, ev->Cookie);
                return;
            }

            if (auto *x = NewInfo.FindPtr(msg->Database)) {
                Send(ev->Sender, new TEvStateStorage::TEvBoardInfo(**x), 0, ev->Cookie);
                return;
            }

            auto &rqstd = Requested[msg->Database];
            if (rqstd.empty()) {
                Register(CreateBoardLookupActor(msg->Database, SelfId(), msg->StateStorageId, EBoardLookupMode::Second, false, false));
            }

            rqstd.push_back({ev->Sender, ev->Cookie});
        }
    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::GRPC_REQ;
        }

        TDiscoveryCache()
            : TActor(&TThis::StateWork)
            , Scheduled(false)
        {}

        STFUNC(StateWork) {
            Y_UNUSED(ctx);
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvPrivate::TEvRequest, Handle);
                hFunc(TEvStateStorage::TEvBoardInfo, Handle);
                cFunc(TEvents::TEvWakeup::EventType, Wakeup);
            }
        }
    };
}

class TListEndpointsRPC : public TActorBootstrapped<TListEndpointsRPC> {
    THolder<TEvListEndpointsRequest> Request;
    const TActorId CacheId;

    const bool RequestScheme = true;

    THolder<TEvStateStorage::TEvBoardInfo> LookupResponse;
    THolder<TEvInterconnect::TEvNodeInfo> NameserviceResponse;
    THolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult> SchemeCacheResponse;

    bool ResolveResources = false;
    ui64 LookupCookie = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    TListEndpointsRPC(TEvListEndpointsRequest::TPtr &msg, TActorId cacheId)
        : Request(msg->Release().Release())
        , CacheId(cacheId)
    {}

    void Bootstrap() {
        // request endpoints
        Lookup(Request->GetProtoRequest()->database());

        // request self node info
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));

        // request path info
        if (RequestScheme) {
            Navigate(Request->GetProtoRequest()->database());
        }

        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        Y_UNUSED(ctx);
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
        }
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr &ev) {
        if (ev->Cookie != LookupCookie) {
            return;
        }

        LookupResponse = THolder<TEvStateStorage::TEvBoardInfo>(ev->Release().Release());

        TryReplyAndDie();
    }

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev) {
        NameserviceResponse = THolder<TEvInterconnect::TEvNodeInfo>(ev->Release().Release());

        TryReplyAndDie();
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr &ev) {
        SchemeCacheResponse = THolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(ev->Release().Release());

        TEvTxProxySchemeCache::TEvNavigateKeySetResult *msg = SchemeCacheResponse.Get();
        NSchemeCache::TSchemeCacheNavigate *navigate = msg->Request.Get();

        Y_VERIFY(navigate->ResultSet.size() == 1);
        const auto &entry = navigate->ResultSet.front();

        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                    "TListEndpointsRPC: handle  TEvNavigateKeySetResult"
                        << ", entry: " << entry.ToString());

        if (navigate->ErrorCount > 0) {
            switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                {
                    auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST, "Requested database not exists");
                    google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
                    NYql::IssueToMessage(issue, issueMessages.Add());
                    Request->SendResult(Ydb::StatusIds::NOT_FOUND, issueMessages);
                    return PassAway();
                }
            default:
                {
                    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                                "TListEndpointsRPC: GENERIC_RESOLVE_ERROR"
                                    << ", entry: " << entry.ToString());

                    auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, "Database resolve failed with no certain result");
                    google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
                    NYql::IssueToMessage(issue, issueMessages.Add());
                    Request->SendResult(Ydb::StatusIds::UNAVAILABLE, issueMessages);
                    return PassAway();
                }
            }
        }

        if (!entry.DomainInfo) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                        "TListEndpointsRPC: GENERIC_RESOLVE_ERROR (empty domain info)"
                            << ", entry: " << entry.ToString());

            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, "Database resolve failed with no certain result");
            google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
            NYql::IssueToMessage(issue, issueMessages.Add());
            Request->SendResult(Ydb::StatusIds::UNAVAILABLE, issueMessages);
            return PassAway();
        }

        auto info = entry.DomainInfo;
        if (info->DomainKey != info->ResourcesDomainKey) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                        "TListEndpointsRPC: domain key differs from resources domain key"
                            << ", domain key: " << info->DomainKey
                            << ", resources domain key: " << info->ResourcesDomainKey);

            Navigate(info->ResourcesDomainKey);
            ResolveResources = true;
        } else if (ResolveResources) {
            Lookup(CanonizePath(entry.Path));
        }

        TryReplyAndDie();
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev) {
        Y_UNUSED(ev);
        auto issue = MakeIssue(NKikimrIssues::TIssuesIds::UNEXPECTED, "Unexpected error while resolving database");
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
        NYql::IssueToMessage(issue, issueMessages.Add());
        Request->SendResult(Ydb::StatusIds::INTERNAL_ERROR, issueMessages);
        return PassAway();
    }

    bool CheckServices(const TSet<TString> &req, const NKikimrStateStorage::TEndpointBoardEntry &entry) {
        if (req.empty())
            return true;

        for (const auto &x : entry.GetServices())
            if (req.count(x))
                return true;

        return false;
    }

    void TryReplyAndDie() {
        if (!NameserviceResponse || !LookupResponse || (RequestScheme && !SchemeCacheResponse))
            return;

        if (RequestScheme) {
            // check presence of database (acl should be checked here too)
            const auto &entry =  SchemeCacheResponse->Request->ResultSet.front();
            if (entry.Path.size() != 1
                && (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindSubdomain && entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindExtSubdomain))
            {
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                             "TListEndpointsRPC: SchemeCacheResponse path is not a database"
                                 << ", entry.Path: " << CanonizePath(entry.Path)
                                 << ", entry.Kind: " << (ui64)entry.Kind);

                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::ACCESS_DENIED, "Requested path is not database name");
                google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
                NYql::IssueToMessage(issue, issueMessages.Add());
                Request->SendResult(Ydb::StatusIds::NOT_FOUND, issueMessages);
                return PassAway();
            }
        }

        if (LookupResponse->Status != TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                        "TListEndpointsRPC: LookupResponse in not OK"
                            << ", LookupResponse->Status: " << ui64(LookupResponse->Status));

            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR, "Database nodes resolve failed with no certain result");
            google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
            NYql::IssueToMessage(issue, issueMessages.Add());
            Request->SendResult(Ydb::StatusIds::UNAVAILABLE, issueMessages);
            return PassAway();
        }

        TStackVec<const TString*> entries;
        entries.reserve(LookupResponse->InfoEntries.size());
        for (auto &xpair : LookupResponse->InfoEntries)
            entries.emplace_back(&xpair.second.Payload);
        Shuffle(entries.begin(), entries.end());

        auto *result = TEvListEndpointsRequest::AllocateResult<Ydb::Discovery::ListEndpointsResult>(Request);
        result->mutable_endpoints()->Reserve(LookupResponse->InfoEntries.size());

        const TSet<TString> services(Request->GetProtoRequest()->Getservice().begin(), Request->GetProtoRequest()->Getservice().end());
        const bool sslServer = Request->SslServer();

        using TEndpointKey = std::pair<TString, ui32>;
        struct TEndpointState {
            int Index = -1;
            int Count = 0;
            float LoadFactor = 0;
            THashSet<TString> Locations;
            THashSet<TString> Services;
        };
        THashMap<TEndpointKey, TEndpointState> states;

        NKikimrStateStorage::TEndpointBoardEntry entry;
        for (const TString *xpayload : entries) {
            Y_PROTOBUF_SUPPRESS_NODISCARD entry.ParseFromString(*xpayload);
            if (!CheckServices(services, entry))
                continue;

            if (entry.GetSsl() != sslServer)
                continue;

            Ydb::Discovery::EndpointInfo *xres;

            auto& state = states[TEndpointKey(entry.GetAddress(), entry.GetPort())];
            if (state.Index >= 0) {
                xres = result->mutable_endpoints(state.Index);
                ++state.Count;
                // FIXME: do we want a mean or a sum here?
                // xres->set_load_factor(xres->load_factor() + (entry.GetLoad() - xres->load_factor()) / state.Count);
                xres->set_load_factor(xres->load_factor() + entry.GetLoad());
            } else {
                state.Index = result->endpoints_size();
                state.Count = 1;
                xres = result->add_endpoints();
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

        auto &nodeInfo = NameserviceResponse->Node;
        if (nodeInfo && nodeInfo->Location.GetDataCenterId()) {
            const auto &location = nodeInfo->Location.GetDataCenterId();
            if (IsSafeLocationMarker(location))
                result->set_self_location(location);
        }


        Request->SendResult(*result, Ydb::StatusIds::SUCCESS);
        PassAway();
    }

    bool IsSafeLocationMarker(TStringBuf location) {
        const ui8* isrc = reinterpret_cast<const ui8*>(location.data());
        for (auto idx : xrange(location.size())) {
            if (isrc[idx] >= 0x80)
                return false;
        }
        return true;
    }

    void Lookup(const TString& db) {
        TVector<TString> path = NKikimr::SplitPath(db);
        auto domainName = path ? path[0] : TString();
        auto *appdata = AppData();
        auto *domainInfo = appdata->DomainsInfo->GetDomainByName(domainName);
        if (!domainInfo) {
            auto issue = MakeIssue(NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST, "Database " + domainName + " not exists");
            google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
            NYql::IssueToMessage(issue, issueMessages.Add());
            Request->SendResult(Ydb::StatusIds::BAD_REQUEST, issueMessages);
            return PassAway();
        }

        TString database;
        for (auto &x : path) {
            if (x.size() > 4100) {
                auto issue = MakeIssue(NKikimrIssues::TIssuesIds::KEY_PARSE_ERROR, "Requested database name too long");
                google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
                NYql::IssueToMessage(issue, issueMessages.Add());
                Request->SendResult(Ydb::StatusIds::BAD_REQUEST, issueMessages);
                return PassAway();
            }
            database.append("/").append(x);
        }

        // request endpoints
        auto stateStorageGroupId = domainInfo->DefaultStateStorageGroup;
        auto reqPath = MakeEndpointsBoardPath(database);

        Send(CacheId, new NDiscoveryPrivate::TEvPrivate::TEvRequest(reqPath, stateStorageGroupId), 0, ++LookupCookie);
        LookupResponse.Reset();
    }

    void FillNavigateKey(const TString& path, NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        entry.Path = NKikimr::SplitPath(path);
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
    }

    void FillNavigateKey(const TPathId& pathId, NSchemeCache::TSchemeCacheNavigate::TEntry& entry) {
        entry.TableId = TTableId(pathId.OwnerId, pathId.LocalPathId);
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
    }

    template <typename T>
    void Navigate(const T& id) {
        LOG_TRACE_S(*TlsActivationContext, NKikimrServices::GRPC_PROXY,
                    "TListEndpointsRPC: make TEvNavigateKeySet request"
                        << ", path: " << id);

        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();

        request->ResultSet.emplace_back();
        FillNavigateKey(id, request->ResultSet.back());
        request->ResultSet.back().Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        request->ResultSet.back().RedirectRequired = false;

        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release()), IEventHandle::FlagTrackDelivery);
        SchemeCacheResponse.Reset();
    }
};

void TGRpcRequestProxy::Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
    if (!DiscoveryCacheActorID)
        DiscoveryCacheActorID = ctx.Register(new NDiscoveryPrivate::TDiscoveryCache());

    ctx.Register(new TListEndpointsRPC(ev, DiscoveryCacheActorID));
}

} // namespace NGRpcService
} // namespace NKikimr
