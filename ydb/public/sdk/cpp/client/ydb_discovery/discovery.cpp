#include "discovery.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>

namespace NYdb {
namespace NDiscovery {

TListEndpointsResult::TListEndpointsResult(TStatus&& status, const Ydb::Discovery::ListEndpointsResult& proto)
    : TStatus(std::move(status))
{
    const auto& endpoints = proto.endpoints();
    Info_.reserve(proto.endpoints().size());
    for (const auto& endpointInfo : endpoints) {
        TEndpointInfo& info = Info_.emplace_back();
        info.Address = endpointInfo.address();
        info.Port = endpointInfo.port();
        info.Location = endpointInfo.location();
        info.NodeId = endpointInfo.node_id();
        info.LoadFactor = endpointInfo.load_factor();
        info.Ssl = endpointInfo.ssl();
        info.Services.reserve(endpointInfo.service().size());
        for (const auto& s : endpointInfo.service()) {
            info.Services.emplace_back(s);
        }
        info.IPv4Addrs.reserve(endpointInfo.ip_v4().size());
        for (const auto& addr : endpointInfo.ip_v4()) {
            info.IPv4Addrs.emplace_back(addr);
        }
        info.IPv6Addrs.reserve(endpointInfo.ip_v6().size());
        for (const auto& addr : endpointInfo.ip_v6()) {
            info.IPv6Addrs.emplace_back(addr);
        }
        info.SslTargetNameOverride = endpointInfo.ssl_target_name_override();
    }
}

const TVector<TEndpointInfo>& TListEndpointsResult::GetEndpointsInfo() const {
    return Info_;
}

TWhoAmIResult::TWhoAmIResult(TStatus&& status, const Ydb::Discovery::WhoAmIResult& proto)
    : TStatus(std::move(status))
{
    UserName_ = proto.user();
    const auto& groups = proto.groups();
    Groups_.reserve(groups.size());
    for (const auto& group : groups) {
        Groups_.emplace_back(group);
    }
}

const TString& TWhoAmIResult::GetUserName() const {
    return UserName_;
}

const TVector<TString>& TWhoAmIResult::GetGroups() const {
    return Groups_;
}

TNodeLocation::TNodeLocation(const Ydb::Discovery::NodeLocation& location)
    : DataCenterNum(location.data_center_num())
    , RoomNum(location.room_num())
    , RackNum(location.rack_num())
    , BodyNum(location.body_num())
    , Body(location.body())
    , DataCenter(location.data_center())
    , Module(location.module())
    , Rack(location.rack())
    , Unit(location.unit())
    {}

TNodeInfo::TNodeInfo(const Ydb::Discovery::NodeInfo& info)
    : NodeId(info.node_id())
    , Host(info.host())
    , Port(info.port())
    , ResolveHost(info.resolve_host())
    , Address(info.address())
    , Location(info.location())
    , Expire(info.expire())
    {}

TNodeRegistrationResult::TNodeRegistrationResult(TStatus&& status, const Ydb::Discovery::NodeRegistrationResult& proto)
    : TStatus(std::move(status))
{
    NodeId_ = proto.node_id();
    DomainPath_ = proto.domain_path();
    Expire_ = proto.expire();
    ScopeTableId_ = proto.scope_tablet_id();
    ScopePathId_ = proto.scope_path_id();
    const auto& nodes = proto.nodes();
    Nodes_.reserve(nodes.size());
    for (const auto& node : nodes) {
        Nodes_.emplace_back(node);
    }
}

const ui32& TNodeRegistrationResult::GetNodeId() const {
    return NodeId_;
}

const TString& TNodeRegistrationResult::GetDomainPath() const {
    return DomainPath_;
}

const ui64& TNodeRegistrationResult::GetExpire() const {
    return Expire_;
}

const ui64& TNodeRegistrationResult::GetScopeTabletId() const {
    return ScopeTableId_;
}

const ui64& TNodeRegistrationResult::GetScopePathId() const {
    return ScopePathId_;
}

const TVector<TNodeInfo>& TNodeRegistrationResult::GetNodes() const {
    return Nodes_;
}

class TDiscoveryClient::TImpl : public TClientImplCommon<TDiscoveryClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    { }

    TAsyncListEndpointsResult ListEndpoints(const TListEndpointsSettings& settings) {
        Ydb::Discovery::ListEndpointsRequest request;
        request.set_database(DbDriverState_->Database);

        auto promise = NThreading::NewPromise<TListEndpointsResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                Ydb::Discovery::ListEndpointsResult result;
                if (any) {
                    any->UnpackTo(&result);
                }
                TListEndpointsResult val{TStatus(std::move(status)), result};
                promise.SetValue(std::move(val));
            };

        Connections_->RunDeferred<Ydb::Discovery::V1::DiscoveryService, Ydb::Discovery::ListEndpointsRequest, Ydb::Discovery::ListEndpointsResponse>(
            std::move(request),
            extractor,
            &Ydb::Discovery::V1::DiscoveryService::Stub::AsyncListEndpoints,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncWhoAmIResult WhoAmI(const TWhoAmISettings& settings) {
        Ydb::Discovery::WhoAmIRequest request;
        if (settings.WithGroups_) {
            request.set_include_groups(true);
        }

        auto promise = NThreading::NewPromise<TWhoAmIResult>();

        auto extractor = [promise]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Discovery::WhoAmIResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            TWhoAmIResult val{ TStatus(std::move(status)), result };
            promise.SetValue(std::move(val));
        };

        Connections_->RunDeferred<Ydb::Discovery::V1::DiscoveryService, Ydb::Discovery::WhoAmIRequest, Ydb::Discovery::WhoAmIResponse>(
            std::move(request),
            extractor,
            &Ydb::Discovery::V1::DiscoveryService::Stub::AsyncWhoAmI,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncNodeRegistrationResult NodeRegistration(const TNodeRegistrationSettings& settings) {
        Ydb::Discovery::NodeRegistrationRequest request;
        request.set_host(settings.Host_);
        request.set_port(settings.Port_);
        request.set_resolve_host(settings.ResolveHost_);
        request.set_address(settings.Address_);
        request.set_domain_path(settings.DomainPath_);
        request.set_fixed_node_id(settings.FixedNodeId_);
        if (!settings.Path_.Empty()) {
            request.set_path(settings.Path_);
        }

        auto requestLocation = request.mutable_location();
        const auto& location = settings.Location_;

        requestLocation->set_data_center(location.DataCenter);
        requestLocation->set_unit(location.Unit);
        requestLocation->set_rack(location.Rack);
        requestLocation->set_unit(location.Unit);

        requestLocation->set_data_center_num(location.DataCenterNum);
        requestLocation->set_room_num(location.RoomNum);
        requestLocation->set_rack_num(location.RackNum);
        requestLocation->set_body_num(location.BodyNum);

        auto promise = NThreading::NewPromise<TNodeRegistrationResult>();

        auto extractor = [promise] (google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Discovery::NodeRegistrationResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            TNodeRegistrationResult val{TStatus(std::move(status)), result};
            promise.SetValue(std::move(val));
        };

        Connections_->RunDeferred<Ydb::Discovery::V1::DiscoveryService, Ydb::Discovery::NodeRegistrationRequest, Ydb::Discovery::NodeRegistrationResponse>(
            std::move(request),
            extractor,
            &Ydb::Discovery::V1::DiscoveryService::Stub::AsyncNodeRegistration,
            DbDriverState_,
            INITIAL_DEFERRED_CALL_DELAY,
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }
};

TDiscoveryClient::TDiscoveryClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{ }

TAsyncListEndpointsResult TDiscoveryClient::ListEndpoints(const TListEndpointsSettings& settings) {
    return Impl_->ListEndpoints(settings);
}

TAsyncWhoAmIResult TDiscoveryClient::WhoAmI(const TWhoAmISettings& settings) {
    return Impl_->WhoAmI(settings);
}

TAsyncNodeRegistrationResult TDiscoveryClient::NodeRegistration(const TNodeRegistrationSettings& settings) {
    return Impl_->NodeRegistration(settings);
}

} // namespace NDiscovery
} // namespace NYdb
