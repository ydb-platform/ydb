#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace Ydb {
namespace Discovery {
    class ListEndpointsResult;
    class WhoAmIResult;
    class NodeRegistrationResult;
    class NodeLocation;
    class NodeInfo;
} // namespace Discovery
} // namespace Ydb

namespace NYdb {
namespace NDiscovery {

////////////////////////////////////////////////////////////////////////////////

class TListEndpointsSettings : public TSimpleRequestSettings<TListEndpointsSettings> {};

struct TWhoAmISettings : public TSimpleRequestSettings<TWhoAmISettings> {
    FLUENT_SETTING_DEFAULT(bool, WithGroups, false);
};

struct TNodeLocation {
    TNodeLocation() = default;
    TNodeLocation(const Ydb::Discovery::NodeLocation& location);

    std::optional<ui32> DataCenterNum;
    std::optional<ui32> RoomNum;
    std::optional<ui32> RackNum;
    std::optional<ui32> BodyNum;
    std::optional<ui32> Body;

    std::optional<TString> DataCenter;
    std::optional<TString> Module;
    std::optional<TString> Rack;
    std::optional<TString> Unit;
};

struct TNodeRegistrationSettings : public TSimpleRequestSettings<TNodeRegistrationSettings> {
    FLUENT_SETTING(TString, Host);
    FLUENT_SETTING(ui32, Port);
    FLUENT_SETTING(TString, ResolveHost);
    FLUENT_SETTING(TString, Address);
    FLUENT_SETTING(TNodeLocation, Location);
    FLUENT_SETTING(TString, DomainPath);
    FLUENT_SETTING_DEFAULT(bool, FixedNodeId, false);
    FLUENT_SETTING(TString, Path);
};

struct TEndpointInfo {
    TString Address;
    ui32 Port = 0;
    float LoadFactor = 0.0;
    bool Ssl = false;
    TVector<TString> Services;
    TString Location;
    ui32 NodeId = 0;
    TVector<TString> IPv4Addrs;
    TVector<TString> IPv6Addrs;
    TString SslTargetNameOverride;
};

class TListEndpointsResult : public TStatus {
public:
    TListEndpointsResult(TStatus&& status, const Ydb::Discovery::ListEndpointsResult& endpoints);
    const TVector<TEndpointInfo>& GetEndpointsInfo() const;
private:
    TVector<TEndpointInfo> Info_;
};

using TAsyncListEndpointsResult = NThreading::TFuture<TListEndpointsResult>;

class TWhoAmIResult : public TStatus {
public:
    TWhoAmIResult(TStatus&& status, const Ydb::Discovery::WhoAmIResult& proto);
    const TString& GetUserName() const;
    const TVector<TString>& GetGroups() const;
private:
    TString UserName_;
    TVector<TString> Groups_;
};

using TAsyncWhoAmIResult = NThreading::TFuture<TWhoAmIResult>;

struct TNodeInfo {
    TNodeInfo() = default;
    TNodeInfo(const Ydb::Discovery::NodeInfo& info);

    ui32 NodeId;
    TString Host;
    ui32 Port;
    TString ResolveHost;
    TString Address;
    TNodeLocation Location;
    ui64 Expire;
};

class TNodeRegistrationResult : public TStatus {
public:
    TNodeRegistrationResult() : TStatus(EStatus::GENERIC_ERROR, NYql::TIssues()) {}
    TNodeRegistrationResult(TStatus&& status, const Ydb::Discovery::NodeRegistrationResult& proto);
    const ui32& GetNodeId() const;
    const TString& GetDomainPath() const;
    const ui64& GetExpire() const;
    const ui64& GetScopeTabletId() const;
    bool HasScopeTabletId() const;
    const ui64& GetScopePathId() const;
    bool HasScopePathId() const;
    const TString& GetNodeName() const;
    bool HasNodeName() const;
    const TVector<TNodeInfo>& GetNodes() const;

private:
    ui32 NodeId_;
    TString DomainPath_;
    ui64 Expire_;
    std::optional<ui64> ScopeTableId_;
    std::optional<ui64> ScopePathId_;
    std::optional<TString> NodeName_;
    TVector<TNodeInfo> Nodes_;
};

using TAsyncNodeRegistrationResult = NThreading::TFuture<TNodeRegistrationResult>;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClient {
public:
    explicit TDiscoveryClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncListEndpointsResult ListEndpoints(const TListEndpointsSettings& settings = TListEndpointsSettings());
    TAsyncWhoAmIResult WhoAmI(const TWhoAmISettings& settings = TWhoAmISettings());
    TAsyncNodeRegistrationResult NodeRegistration(const TNodeRegistrationSettings& settings = TNodeRegistrationSettings());

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NTable
} // namespace NDiscovery
