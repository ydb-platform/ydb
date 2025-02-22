#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYdb::inline V2::NDebug {

////////////////////////////////////////////////////////////////////////////////

class TPlainGrpcPingResult: public TStatus {
public:
    TPlainGrpcPingResult(TStatus&& status)
        : TStatus(std::move(status))
    {}
};

class TGrpcProxyPingResult: public TStatus {
public:
    TGrpcProxyPingResult(TStatus&& status)
        : TStatus(std::move(status))
    {}
};

class TKqpProxyPingResult: public TStatus {
public:
    TKqpProxyPingResult(TStatus&& status)
        : TStatus(std::move(status))
    {}
};

class TSchemeCachePingResult: public TStatus {
public:
    TSchemeCachePingResult(TStatus&& status)
        : TStatus(std::move(status))
    {}
};

class TTxProxyPingResult: public TStatus {
public:
    TTxProxyPingResult(TStatus&& status)
        : TStatus(std::move(status))
    {}
};

////////////////////////////////////////////////////////////////////////////////

using TAsyncPlainGrpcPingResult = NThreading::TFuture<TPlainGrpcPingResult>;
using TAsyncGrpcProxyPingResult = NThreading::TFuture<TGrpcProxyPingResult>;
using TAsyncKqpProxyPingResult = NThreading::TFuture<TKqpProxyPingResult>;
using TAsyncSchemeCachePingResult = NThreading::TFuture<TSchemeCachePingResult>;
using TAsyncTxProxyPingResult = NThreading::TFuture<TTxProxyPingResult>;

////////////////////////////////////////////////////////////////////////////////

struct TPlainGrpcPingSettings : public TOperationRequestSettings<TPlainGrpcPingSettings> {};
struct TGrpcProxyPingSettings : public TOperationRequestSettings<TGrpcProxyPingSettings> {};
struct TKqpProxyPingSettings : public TOperationRequestSettings<TKqpProxyPingSettings> {};
struct TSchemeCachePingSettings : public TOperationRequestSettings<TSchemeCachePingSettings> {};
struct TTxProxyPingSettings : public TOperationRequestSettings<TTxProxyPingSettings> {};

////////////////////////////////////////////////////////////////////////////////

struct TClientSettings : public TCommonClientSettingsBase<TClientSettings> {
};

class TDebugClient {
public:
    using TAsyncPlainGrpcPingResult = TAsyncPlainGrpcPingResult;
public:
    TDebugClient(const TDriver& driver, const TClientSettings& settings = TClientSettings());

    TAsyncPlainGrpcPingResult PingPlainGrpc(const TPlainGrpcPingSettings& settings);
    TAsyncGrpcProxyPingResult PingGrpcProxy(const TGrpcProxyPingSettings& settings);
    TAsyncKqpProxyPingResult PingKqpProxy(const TKqpProxyPingSettings& settings);

    TAsyncSchemeCachePingResult PingSchemeCache(const TSchemeCachePingSettings& settings);
    TAsyncTxProxyPingResult PingTxProxy(const TTxProxyPingSettings& settings);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NDebug
