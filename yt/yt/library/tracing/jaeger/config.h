#pragma once

#include "public.h"

#include <yt/yt/library/tvm/service/config.h>
#include <yt/yt/library/tvm/service/public.h>

#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

struct TSamplerConfig
    : public NYTree::TYsonStruct
{
    //! Request is sampled with probability P.
    double GlobalSampleRate;

    //! Additionally, request is sampled with probability P(user).
    THashMap<std::string, double> UserSampleRate;

    //! Spans are sent to specified endpoint.
    THashMap<std::string, std::string> UserEndpoint;

    //! Additionally, sample first N requests for each user in the window.
    ui64 MinPerUserSamples;
    TDuration MinPerUserSamplesPeriod;

    //! Clear sampled from from incoming user request.
    THashMap<std::string, bool> ClearSampledFlag;

    REGISTER_YSON_STRUCT(TSamplerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSamplerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJaegerTracerDynamicConfig
    : public NYTree::TYsonStruct
{
    NRpc::NGrpc::TChannelConfigPtr CollectorChannel;

    std::optional<i64> MaxRequestSize;

    std::optional<i64> MaxMemory;

    std::optional<double> SubsamplingRate;

    std::optional<TDuration> FlushPeriod;

    REGISTER_YSON_STRUCT(TJaegerTracerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJaegerTracerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJaegerTracerConfig
    : public NYTree::TYsonStruct
{
    NRpc::NGrpc::TChannelConfigPtr CollectorChannelConfig;

    TDuration FlushPeriod;

    TDuration StopTimeout;

    TDuration RpcTimeout;

    TDuration EndpointChannelTimeout;

    TDuration QueueStallTimeout;

    TDuration ReconnectPeriod;

    i64 MaxRequestSize;

    i64 MaxBatchSize;

    i64 MaxMemory;

    std::optional<double> SubsamplingRate;

    // ServiceName is required by jaeger. When ServiceName is missing, tracer is disabled.
    std::optional<std::string> ServiceName;

    THashMap<std::string, std::string> ProcessTags;

    bool EnablePidTag;

    NAuth::TTvmServiceConfigPtr TvmService;

    // Does not send spans to a collector, but just drops them instead. Logs batch and span count.
    bool TestDropSpans;

    TJaegerTracerConfigPtr ApplyDynamic(const TJaegerTracerDynamicConfigPtr& dynamicConfig) const;

    bool IsEnabled() const;

    REGISTER_YSON_STRUCT(TJaegerTracerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJaegerTracerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
