#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYdb::NDebug {

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


////////////////////////////////////////////////////////////////////////////////

using TAsyncPlainGrpcPingResult = NThreading::TFuture<TPlainGrpcPingResult>;
using TAsyncGrpcProxyPingResult = NThreading::TFuture<TGrpcProxyPingResult>;
using TAsyncKqpProxyPingResult = NThreading::TFuture<TKqpProxyPingResult>;

////////////////////////////////////////////////////////////////////////////////

struct TPlainGrpcPingSettings : public TOperationRequestSettings<TPlainGrpcPingSettings> {};
struct TGrpcProxyPingSettings : public TOperationRequestSettings<TGrpcProxyPingSettings> {};
struct TKqpProxyPingSettings : public TOperationRequestSettings<TKqpProxyPingSettings> {};

struct TClientSettings : public TCommonClientSettingsBase<TClientSettings> {
};

class TDebugClient {
public:
    using TAsyncPlainGrpcPingResult = TAsyncPlainGrpcPingResult;
public:
    TDebugClient(const TDriver& driver, const TClientSettings& settings = TClientSettings());

    TAsyncPlainGrpcPingResult PlainGrpcPing(const TPlainGrpcPingSettings& settings);
    TAsyncGrpcProxyPingResult GrpcProxyPing(const TGrpcProxyPingSettings& settings);
    TAsyncKqpProxyPingResult KqpProxyPing(const TKqpProxyPingSettings& settings);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NDebug
