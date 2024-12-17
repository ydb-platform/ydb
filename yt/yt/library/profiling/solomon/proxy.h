#pragma once

#include "public.h"
#include "helpers.h"

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/cgiparam/cgiparam.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointProvider
    : public TRefCounted
{
    struct TEndpoint
    {
        //! Logical name of the endpoint.
        TString Name;
        //! Actual address to pull sensors from.
        TString Address;
        //! A set of labels which can be filtered upon.
        THashMap<TString, TString> Labels;
    };

    //! Human-readable component name.
    virtual TString GetComponentName() const = 0;
    //! This method is allowed to return a cached response.
    virtual std::vector<TEndpoint> GetEndpoints() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IEndpointProvider)

////////////////////////////////////////////////////////////////////////////////

struct TSolomonProxyConfig
    : public NYTree::TYsonStruct
{
    //! A list of publicly available components.
    //! Endpoints from other components are not pulled from.
    //! Currently, this filter is applied for all requests.
    //! TODO(achulkov2): Add some form of access control to allow bypassing this check.
    std::optional<THashSet<TString>> PublicComponentNames;

    //! Forbids pulling sensors from too many endpoints within a single request.
    //! Instead of increasing this limit, one should make requests with a larger shard count.
    int MaxEndpointsPerRequest;

    REGISTER_YSON_STRUCT(TSolomonProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSolomonProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TSolomonProxy
    : public TRefCounted
{
public:
    TSolomonProxy(TSolomonProxyConfigPtr config, NConcurrency::IPollerPtr poller);

    void RegisterEndpointProvider(const IEndpointProviderPtr& endpointProvider);
    void UnregisterEndpointProvider(const IEndpointProviderPtr& endpointProvider);

    void Register(const TString& prefix, const NHttp::IServerPtr& server);

private:
    const TSolomonProxyConfigPtr Config_;
    const NHttp::IClientPtr HttpClient_;
    THashMap<TString, IEndpointProviderPtr> ComponentNameToEndpointProvider_;

    void HandleSensors(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);
    void GuardedHandleSensors(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);

    static void ValidateShardingParameters(int shardIndex, int shardCount);

    struct TComponentMatcher
    {
        std::optional<THashSet<TString>> Components;
    };

    static TComponentMatcher GetComponentMatcher(const TCgiParameters& parameters);
    static bool MatchComponent(const TComponentMatcher& componentMatcher, const IEndpointProviderPtr& endpointProvider);

    struct TInstanceFilter
    {
        std::optional<THashSet<TString>> Instances;
        THashMap<TString, THashSet<TString>> LabelFilter;
    };

    static TInstanceFilter GetInstanceFilter(const TCgiParameters& parameters);
    static std::vector<IEndpointProvider::TEndpoint> FilterInstances(
        const TInstanceFilter& instanceFilter,
        const std::vector<IEndpointProvider::TEndpoint>& endpoints);

    std::vector<TString> CollectEndpoints(const TCgiParameters& parameters, int shardIndex, int shardCount) const;

    //! For safety we only proxy whitelisted headers.
    static NHttp::THeadersPtr PreparePullHeaders(const NHttp::THeadersPtr& reqHeaders, const TOutputEncodingContext& outputContext);
    //! For safety we only proxy whitelisted parameters.
    static TCgiParameters PreparePullParameters(const TCgiParameters& parameters);
};

DEFINE_REFCOUNTED_TYPE(TSolomonProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
