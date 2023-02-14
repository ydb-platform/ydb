#include "grpc_request_proxy.h"

#include "rpc_calls.h"
#include "rpc_kqp_base.h"

#include <ydb/core/base/discovery.h>
#include <ydb/core/base/location.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/interconnect/interconnect.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/random/shuffle.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;
using namespace NKqp;

class TListEndpointsRPC : public TActorBootstrapped<TListEndpointsRPC> {
    THolder<TEvListEndpointsRequest> Request;
    const TActorId CacheId;
    TActorId Discoverer;

    THolder<TEvStateStorage::TEvBoardInfo> LookupResponse;
    THolder<TEvInterconnect::TEvNodeInfo> NameserviceResponse;

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
        Discoverer = Register(CreateDiscoverer(Request->GetProtoRequest()->database(), SelfId(), CacheId));

        // request self node info
        Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(SelfId().NodeId()));

        Become(&TThis::StateWait);
    }

    STATEFN(StateWait) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, Handle);
            hFunc(TEvInterconnect::TEvNodeInfo, Handle);
            hFunc(TEvDiscovery::TEvError, Handle);
        }
    }

    void Handle(TEvStateStorage::TEvBoardInfo::TPtr &ev) {
        LookupResponse.Reset(ev->Release().Release());
        TryReplyAndDie();
    }

    void Handle(TEvInterconnect::TEvNodeInfo::TPtr &ev) {
        NameserviceResponse.Reset(ev->Release().Release());
        TryReplyAndDie();
    }

    void Handle(TEvDiscovery::TEvError::TPtr &ev) {
        auto issue = MakeIssue(ErrorToIssueCode(ev->Get()->Status), ev->Get()->Error);
        google::protobuf::RepeatedPtrField<TYdbIssueMessageType> issueMessages;
        NYql::IssueToMessage(issue, issueMessages.Add());
        Request->SendResult(ErrorToStatusCode(ev->Get()->Status), issueMessages);
        PassAway();
    }

    static NKikimrIssues::TIssuesIds::EIssueCode ErrorToIssueCode(TEvDiscovery::TEvError::EStatus status) {
        switch (status) {
            case TEvDiscovery::TEvError::KEY_PARSE_ERROR: return NKikimrIssues::TIssuesIds::KEY_PARSE_ERROR;
            case TEvDiscovery::TEvError::RESOLVE_ERROR: return NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR;
            case TEvDiscovery::TEvError::DATABASE_NOT_EXIST: return NKikimrIssues::TIssuesIds::DATABASE_NOT_EXIST;
            case TEvDiscovery::TEvError::ACCESS_DENIED: return NKikimrIssues::TIssuesIds::ACCESS_DENIED;
            default: return NKikimrIssues::TIssuesIds::DEFAULT_ERROR;
        }
    }

    static Ydb::StatusIds::StatusCode ErrorToStatusCode(TEvDiscovery::TEvError::EStatus status) {
        switch (status) {
            case TEvDiscovery::TEvError::KEY_PARSE_ERROR: return Ydb::StatusIds::BAD_REQUEST;
            case TEvDiscovery::TEvError::RESOLVE_ERROR: return Ydb::StatusIds::UNAVAILABLE;
            case TEvDiscovery::TEvError::DATABASE_NOT_EXIST: return Ydb::StatusIds::NOT_FOUND;
            case TEvDiscovery::TEvError::ACCESS_DENIED: return Ydb::StatusIds::NOT_FOUND;
            default: return Ydb::StatusIds::BAD_REQUEST;
        }
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
        if (!NameserviceResponse || !LookupResponse)
            return;

        Y_VERIFY(LookupResponse->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok);

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
};

void TGRpcRequestProxy::Handle(TEvListEndpointsRequest::TPtr& ev, const TActorContext& ctx) {
    if (!DiscoveryCacheActorID) {
        DiscoveryCacheActorID = ctx.Register(CreateDiscoveryCache());
    }

    ctx.Register(new TListEndpointsRPC(ev, DiscoveryCacheActorID));
}

}
