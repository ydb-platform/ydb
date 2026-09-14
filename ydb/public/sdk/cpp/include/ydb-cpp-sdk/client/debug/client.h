#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::inline Dev::NDebug {

////////////////////////////////////////////////////////////////////////////////

class TPlainGrpcPingResult: public TStatus {
public:
    TPlainGrpcPingResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    ui64 CallBackTs = 0;
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

class TActorChainPingResult: public TStatus {
public:
    TActorChainPingResult(TStatus&& status)
        : TStatus(std::move(status))
    {}
};

////////////////////////////////////////////////////////////////////////////////

using TAsyncPlainGrpcPingResult = NThreading::TFuture<TPlainGrpcPingResult>;
using TAsyncGrpcProxyPingResult = NThreading::TFuture<TGrpcProxyPingResult>;
using TAsyncKqpProxyPingResult = NThreading::TFuture<TKqpProxyPingResult>;
using TAsyncSchemeCachePingResult = NThreading::TFuture<TSchemeCachePingResult>;
using TAsyncTxProxyPingResult = NThreading::TFuture<TTxProxyPingResult>;
using TAsyncActorChainPingResult = NThreading::TFuture<TActorChainPingResult>;

////////////////////////////////////////////////////////////////////////////////

struct TPlainGrpcPingSettings : public TSimpleRequestSettings<TPlainGrpcPingSettings> {};
struct TGrpcProxyPingSettings : public TSimpleRequestSettings<TGrpcProxyPingSettings> {};
struct TKqpProxyPingSettings : public TSimpleRequestSettings<TKqpProxyPingSettings> {};
struct TSchemeCachePingSettings : public TSimpleRequestSettings<TSchemeCachePingSettings> {};
struct TTxProxyPingSettings : public TSimpleRequestSettings<TTxProxyPingSettings> {};

struct TActorChainPingSettings : public TSimpleRequestSettings<TActorChainPingSettings> {
    FLUENT_SETTING_DEFAULT(size_t, ChainLength, 10);
    FLUENT_SETTING_DEFAULT(size_t, WorkUsec, 5);
    FLUENT_SETTING_DEFAULT(bool, NoTailChain, false);
};

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

    TAsyncActorChainPingResult PingActorChain(const TActorChainPingSettings& settings);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NDebug
