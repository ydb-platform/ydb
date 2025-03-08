#pragma once

#include <ydb-cpp-sdk/client/driver/driver.h>

namespace Ydb {
namespace Discovery {
    class ListEndpointsResult;
    class WhoAmIResult;
    class NodeRegistrationResult;
    class NodeLocation;
    class NodeInfo;
} // namespace Discovery
} // namespace Ydb

namespace NYdb::inline Dev {
namespace NDiscovery {

////////////////////////////////////////////////////////////////////////////////

class TListEndpointsSettings : public TSimpleRequestSettings<TListEndpointsSettings> {};

struct TWhoAmISettings : public TSimpleRequestSettings<TWhoAmISettings> {
    FLUENT_SETTING_DEFAULT(bool, WithGroups, false);
};

struct TNodeLocation {
    TNodeLocation() = default;
    TNodeLocation(const Ydb::Discovery::NodeLocation& location);

    std::optional<uint32_t> DataCenterNum;
    std::optional<uint32_t> RoomNum;
    std::optional<uint32_t> RackNum;
    std::optional<uint32_t> BodyNum;
    std::optional<uint32_t> Body;

    std::optional<std::string> DataCenter;
    std::optional<std::string> Module;
    std::optional<std::string> Rack;
    std::optional<std::string> Unit;
};

struct TNodeRegistrationSettings : public TSimpleRequestSettings<TNodeRegistrationSettings> {
    FLUENT_SETTING(std::string, Host);
    FLUENT_SETTING(uint32_t, Port);
    FLUENT_SETTING(std::string, ResolveHost);
    FLUENT_SETTING(std::string, Address);
    FLUENT_SETTING(TNodeLocation, Location);
    FLUENT_SETTING(std::string, DomainPath);
    FLUENT_SETTING_DEFAULT(bool, FixedNodeId, false);
    FLUENT_SETTING(std::string, Path);
};

struct TEndpointInfo {
    std::string Address;
    uint32_t Port = 0;
    float LoadFactor = 0.0;
    bool Ssl = false;
    std::vector<std::string> Services;
    std::string Location;
    uint32_t NodeId = 0;
    std::vector<std::string> IPv4Addrs;
    std::vector<std::string> IPv6Addrs;
    std::string SslTargetNameOverride;
};

class TListEndpointsResult : public TStatus {
public:
    TListEndpointsResult(TStatus&& status, const Ydb::Discovery::ListEndpointsResult& endpoints);
    const std::vector<TEndpointInfo>& GetEndpointsInfo() const;
private:
    std::vector<TEndpointInfo> Info_;
};

using TAsyncListEndpointsResult = NThreading::TFuture<TListEndpointsResult>;

class TWhoAmIResult : public TStatus {
public:
    TWhoAmIResult(TStatus&& status, const Ydb::Discovery::WhoAmIResult& proto);
    const std::string& GetUserName() const;
    const std::vector<std::string>& GetGroups() const;
private:
    std::string UserName_;
    std::vector<std::string> Groups_;
};

using TAsyncWhoAmIResult = NThreading::TFuture<TWhoAmIResult>;

struct TNodeInfo {
    TNodeInfo() = default;
    TNodeInfo(const Ydb::Discovery::NodeInfo& info);

    uint32_t NodeId;
    std::string Host;
    uint32_t Port;
    std::string ResolveHost;
    std::string Address;
    TNodeLocation Location;
    uint64_t Expire;
};

class TNodeRegistrationResult : public TStatus {
public:
    TNodeRegistrationResult() : TStatus(EStatus::GENERIC_ERROR, NYdb::NIssue::TIssues()) {}
    TNodeRegistrationResult(TStatus&& status, const Ydb::Discovery::NodeRegistrationResult& proto);
    uint32_t GetNodeId() const;
    const std::string& GetDomainPath() const;
    uint64_t GetExpire() const;
    uint64_t GetScopeTabletId() const;
    bool HasScopeTabletId() const;
    uint64_t GetScopePathId() const;
    bool HasScopePathId() const;
    const std::string& GetNodeName() const;
    bool HasNodeName() const;
    const std::vector<TNodeInfo>& GetNodes() const;

private:
    uint32_t NodeId_;
    std::string DomainPath_;
    uint64_t Expire_;
    std::optional<uint64_t> ScopeTableId_;
    std::optional<uint64_t> ScopePathId_;
    std::optional<std::string> NodeName_;
    std::vector<TNodeInfo> Nodes_;
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

} // namespace NDiscovery
} // namespace NYdb
