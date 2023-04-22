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

} // namespace NDiscovery
} // namespace NYdb
