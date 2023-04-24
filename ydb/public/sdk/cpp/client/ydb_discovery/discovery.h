#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace Ydb {
namespace Discovery {
    class ListEndpointsResult;
    class WhoAmIResult;
} // namespace Discovery
} // namespace Ydb

namespace NYdb {
namespace NDiscovery {

////////////////////////////////////////////////////////////////////////////////

class TListEndpointsSettings : public TSimpleRequestSettings<TListEndpointsSettings> {};

struct TWhoAmISettings : public TSimpleRequestSettings<TWhoAmISettings> {
    FLUENT_SETTING_DEFAULT(bool, WithGroups, false);
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

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryClient {
public:
    explicit TDiscoveryClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    TAsyncListEndpointsResult ListEndpoints(const TListEndpointsSettings& settings = TListEndpointsSettings());
    TAsyncWhoAmIResult WhoAmI(const TWhoAmISettings& settings = TWhoAmISettings());

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NTable
} // namespace NDiscovery
