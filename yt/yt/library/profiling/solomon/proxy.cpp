#include "proxy.h"
#include "private.h"

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>
#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/prometheus/prometheus.h>

namespace NYT::NProfiling {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;
using namespace NLogging;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = SolomonLogger;

////////////////////////////////////////////////////////////////////////////////

static const TString ShardIndexParameterName("shard_index");
static const TString ShardCountParameterName("shard_count");
static const TString ComponentParameterName("component");
static const TString InstanceParameterName("instance");

static const TString InstanceLabelParameterNamePrefix("instance_");

static const std::vector<TString> ForwardParameterWhitelist = {
    "period",
    "now",
};

static const std::vector<TString> ForwardHeaderWhitelist = {
    "X-Solomon-GridSec",
    "X-Solomon-ClusterId",
};

static constexpr int PullErrorSampleSize = 20;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

int ParseIntegerParameter(const TCgiParameters& parameters, const TString& parameterName, int defaultValue)
{
    int value = defaultValue;

    if (auto it = parameters.Find(parameterName); it != parameters.end()) {
        if (!TryFromString<int>(it->second, value)) {
            THROW_ERROR_EXCEPTION("Invalid value of %Qv parameter", parameterName)
                << TErrorAttribute("value", it->second);
        }
    }

    return value;
}

std::optional<TString> TrimPrefix(const TString& name, const TString& prefix)
{
    if (name.StartsWith(prefix)) {
        return name.substr(prefix.size());
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TSolomonProxyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("public_component_names", &TThis::PublicComponentNames)
        .Default();
    registrar.Parameter("max_endpoints_per_request", &TThis::MaxEndpointsPerRequest)
        .Default(30);
}

////////////////////////////////////////////////////////////////////////////////

TSolomonProxy::TSolomonProxy(TSolomonProxyConfigPtr config, IPollerPtr poller)
    : Config_(std::move(config))
    , HttpClient_(CreateClient(New<TClientConfig>(), std::move(poller)))
{ }

void TSolomonProxy::RegisterEndpointProvider(const IEndpointProviderPtr& endpointProvider)
{
    EmplaceOrCrash(ComponentNameToEndpointProvider_, endpointProvider->GetComponentName(), endpointProvider);
}

void TSolomonProxy::UnregisterEndpointProvider(const IEndpointProviderPtr& endpointProvider)
{
    EraseOrCrash(ComponentNameToEndpointProvider_, endpointProvider->GetComponentName());
}

void TSolomonProxy::Register(const TString& prefix, const IServerPtr& server)
{
    server->AddHandler(prefix + "/sensors", BIND(&TSolomonProxy::HandleSensors, MakeStrong(this)));
    // TODO(achulkov2): Expose available tags/targets?
}

void TSolomonProxy::ValidateShardingParameters(int shardIndex, int shardCount)
{
    if (!(0 <= shardIndex && shardIndex < shardCount)) {
        THROW_ERROR_EXCEPTION(
            "Invalid sharding configuration, %v=%v must be in range [0, %v=%v)",
            ShardIndexParameterName,
            shardIndex,
            ShardCountParameterName,
            shardCount);
    }
}

THeadersPtr TSolomonProxy::PreparePullHeaders(const THeadersPtr& reqHeaders, const TOutputEncodingContext& outputContext)
{
    auto pullHeaders = New<THeaders>();
    // We use this format since it is the only one that supports compression.
    pullHeaders->Set("Accept", TString(::NMonitoring::ContentTypeByFormat(::NMonitoring::EFormat::SPACK)));
    pullHeaders->Set("Accept-Encoding", TString(::NMonitoring::ContentEncodingByCompression(::NMonitoring::ECompression::ZSTD)));
    // We use a separate header to propagate whether or not this was a "solomon" pull. It affects counter-to-rate conversion.
    pullHeaders->Set(IsSolomonPullHeaderName, outputContext.IsSolomonPull ? "1" : "0");

    for (const auto& forwardHeader : ForwardHeaderWhitelist) {
        if (auto value = reqHeaders->Find(forwardHeader)) {
            pullHeaders->Set(forwardHeader, *value);
        }
    }

    return pullHeaders;
}

TCgiParameters TSolomonProxy::PreparePullParameters(const TCgiParameters& parameters)
{
    TCgiParameters pullParameters;
    for (const auto& forwardParamater : ForwardParameterWhitelist) {
        if (auto valueIt = parameters.find(forwardParamater); valueIt != parameters.end()) {
            pullParameters.insert(*valueIt);
        }
    }

    return pullParameters;
}

TSolomonProxy::TComponentMatcher TSolomonProxy::GetComponentMatcher(const TCgiParameters& parameters)
{
    TComponentMatcher componentMatcher;

    if (parameters.Has(ComponentParameterName)) {
        componentMatcher.Components.emplace();
    }

    for (const auto& componentName : parameters.Range(ComponentParameterName)) {
        componentMatcher.Components->insert(componentName);
    }

    return componentMatcher;
}

bool TSolomonProxy::MatchComponent(const TComponentMatcher& componentMatcher, const IEndpointProviderPtr& endpointProvider)
{
    return !componentMatcher.Components || componentMatcher.Components->contains(endpointProvider->GetComponentName());
}

TSolomonProxy::TInstanceFilter TSolomonProxy::GetInstanceFilter(const TCgiParameters& parameters)
{
    TInstanceFilter instanceFilter;

    for (const auto& [name, value] : parameters) {
        if (name == InstanceParameterName) {
            if (!instanceFilter.Instances) {
                instanceFilter.Instances.emplace();
            }

            instanceFilter.Instances->insert(value);
        } else if (auto instanceParameterName = TrimPrefix(name, InstanceLabelParameterNamePrefix)) {
            instanceFilter.LabelFilter[*instanceParameterName].insert(value);
        }
    }

    return instanceFilter;
}

std::vector<IEndpointProvider::TEndpoint> TSolomonProxy::FilterInstances(
    const TInstanceFilter& instanceFilter,
    const std::vector<IEndpointProvider::TEndpoint>& endpoints)
{
    std::vector<IEndpointProvider::TEndpoint> filteredEndpoints;
    filteredEndpoints.reserve(endpoints.size());

    for (const auto& endpoint : endpoints) {
        // Skip non-selected instances if instance names are specified.
        if (instanceFilter.Instances && !instanceFilter.Instances->contains(endpoint.Name)) {
            continue;
        }

        // For all specified labels, skip instances that do not have the selected values of these labels (or any value).
        bool matchesLabelFilter = true;
        for (const auto& [label, acceptableValues] : instanceFilter.LabelFilter) {
            auto instanceLabelValueIt = endpoint.Labels.find(label);
            if (instanceLabelValueIt == endpoint.Labels.end() || !acceptableValues.contains(instanceLabelValueIt->second)) {
                matchesLabelFilter = false;
                break;
            }
        }
        if (!matchesLabelFilter) {
            continue;
        }

        filteredEndpoints.push_back(endpoint);
    }

    return filteredEndpoints;
}

std::vector<TString> TSolomonProxy::CollectEndpoints(const TCgiParameters& parameters, int shardIndex, int shardCount) const
{
    auto componentMatcher = GetComponentMatcher(parameters);
    auto instanceFilter = GetInstanceFilter(parameters);

    YT_LOG_DEBUG(
        "Selecting endpoints (PublicComponentNames: %v, RequestedComponents: %v, RequestedInstances: %v, InstanceLabelFilter: %v)",
        Config_->PublicComponentNames,
        componentMatcher.Components,
        instanceFilter.Instances,
        instanceFilter.LabelFilter);

    int selectedComponents = 0;
    int skippedByInternalFilter = 0;
    int skippedByUserFilter = 0;

    std::vector<TString> allEndpoints;

    for (const auto& [componentName, endpointProvider] : ComponentNameToEndpointProvider_) {
        YT_VERIFY(componentName == endpointProvider->GetComponentName());

        if (Config_->PublicComponentNames && !Config_->PublicComponentNames->contains(componentName)) {
            ++skippedByInternalFilter;
            YT_LOG_DEBUG("Skipping non-public component (ComponentName: %v)", componentName);
            continue;
        }

        if (!MatchComponent(componentMatcher, endpointProvider)) {
            ++skippedByUserFilter;
            YT_LOG_DEBUG("Skipping non-requested component (ComponentName: %v)", componentName);
            continue;
        }

        ++selectedComponents;

        auto componentEndpoints = endpointProvider->GetEndpoints();
        auto selectedEndpointCountBefore = allEndpoints.size();
        auto filteredInstances = FilterInstances(instanceFilter, componentEndpoints);
        for (const auto& endpoint : filteredInstances) {
            if (FarmFingerprint(endpoint.Address) % shardCount == shardIndex) {
                allEndpoints.push_back(endpoint.Address);
            }
        }

        // For debug purposes.
        int shardEndpointCount = 0;
        for (const auto& endpoint : componentEndpoints) {
            if (FarmFingerprint(endpoint.Address) % shardCount == shardIndex) {
                ++shardEndpointCount;
            }
        }

        YT_LOG_DEBUG(
            "Selected component endpoints for pull (ComponentName: %v, SelectedEndpointCount: %v, TotalEndpointCount: %v, InstanceFilterMatchCount: %v, ShardEndpointCount: %v)",
            componentName,
            allEndpoints.size() - selectedEndpointCountBefore,
            componentEndpoints.size(),
            filteredInstances.size(),
            shardEndpointCount);
    }

    YT_LOG_DEBUG(
        "Collected endpoints from components (EndpointCount: %v, ComponentCount: %v, ComponentSkippedByInternalFilterCount: %v, ComponentSkippedByUserFilterCount: %v)",
        allEndpoints.size(),
        selectedComponents,
        skippedByInternalFilter,
        skippedByUserFilter);

    return allEndpoints;
}

void TSolomonProxy::HandleSensors(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    try {
        GuardedHandleSensors(req, rsp);
    } catch(const std::exception& ex) {
        YT_LOG_DEBUG(ex, "Failed to pull sensors from endpoints");

        if (!rsp->AreHeadersFlushed()) {
            try {
                rsp->SetStatus(EStatusCode::InternalServerError);
                rsp->GetHeaders()->Remove("Content-Type");
                rsp->GetHeaders()->Remove("Content-Encoding");

                ReplyError(rsp, ex);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to send sensor pull error");
            }
        }
    }
}

void TSolomonProxy::GuardedHandleSensors(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    auto reqUrlRef = req->GetUrl();
    TCgiParameters parameters(reqUrlRef.RawQuery);

    auto shardIndex = ParseIntegerParameter(parameters, ShardIndexParameterName, /*defaultValue*/ 0);
    auto shardCount = ParseIntegerParameter(parameters, ShardCountParameterName, /*defaultValue*/ 1);

    YT_LOG_DEBUG(
        "Performing solomon proxy fetch (ShardIndex: %v, ShardCount: %v, Query: %v)",
        shardIndex,
        shardCount,
        reqUrlRef.RawQuery);

    ValidateShardingParameters(shardIndex, shardCount);

    auto outputEncodingContext = CreateOutputEncodingContextFromHeaders(req->GetHeaders());

    auto filteredEndpoints = CollectEndpoints(parameters, shardIndex, shardCount);

    // TODO(achulkov2): Ideally, we would want some sort of memory accounting here.
    if (std::ssize(filteredEndpoints) > Config_->MaxEndpointsPerRequest) {
        THROW_ERROR_EXCEPTION("Cannot pull sensors from %v endpoints at once, please retry the request with a larger shard count", filteredEndpoints.size())
            << TErrorAttribute("max_endpoints_per_request", Config_->MaxEndpointsPerRequest);
    }

    std::vector<TFuture<IResponsePtr>> asyncPullResponses;
    asyncPullResponses.reserve(filteredEndpoints.size());

    auto pullHeaders = PreparePullHeaders(req->GetHeaders(), outputEncodingContext);
    auto pullParameters = PreparePullParameters(parameters);

    YT_LOG_DEBUG(
        "Proxying pull requests to hosts (EndpointCount: %v, Parameters: %v, Headers: %v)",
        filteredEndpoints.size(),
        // This is easier to read than the escaped string.
        std::vector<std::pair<TString, TString>>(pullParameters.begin(), pullParameters.end()),
        pullHeaders->Dump());

    for (auto pullUrl : filteredEndpoints) {
        if (!pullParameters.empty()) {
            pullUrl += Format("?%v", pullParameters.Print());
        }

        asyncPullResponses.push_back(HttpClient_->Get(pullUrl, pullHeaders));
    }

    auto pullResponseOrErrors = WaitFor(AllSet(asyncPullResponses))
        .ValueOrThrow();

    std::vector<TError> pullErrors;

    for (const auto& pullResponseOrError : pullResponseOrErrors) {
        if (!pullResponseOrError.IsOK()) {
            YT_LOG_DEBUG(pullResponseOrError, "Error while pulling sensors from endpoint");
            pullErrors.push_back(pullResponseOrError);
            continue;
        }

        const auto& pullResponse = pullResponseOrError.Value();
        auto body = pullResponse->ReadAll();

        if (pullResponse->GetStatusCode() != EStatusCode::OK) {
            YT_LOG_DEBUG("Sensor pull failed (StatusCode: %v, Response: %v)", pullResponse->GetStatusCode(), body);
            pullErrors.push_back(
                TError("Sensor pull failed with status code %v", pullResponse->GetStatusCode())
                    << TErrorAttribute("response_body", ToString(body))
                    << TErrorAttribute("status_code", pullResponse->GetStatusCode()));
            continue;
        }

        TMemoryInput stream(body.Begin(), body.Size());
        ::NMonitoring::DecodeSpackV1(&stream, outputEncodingContext.Encoder.Get());
    }

    YT_LOG_DEBUG(
        "Pulled sensors from endpoints (RequestCount: %v, SuccessCount: %v, FailureCount: %v)",
        asyncPullResponses.size(),
        asyncPullResponses.size() - pullErrors.size(),
        pullErrors.size());

    // TODO(achulkov2): Think about more introspection of pull failures for other cases.
    if (!pullErrors.empty() && pullErrors.size() == asyncPullResponses.size()) {
        auto totalErrorCount = pullErrors.size();
        if (std::ssize(pullErrors) > PullErrorSampleSize) {
            pullErrors.resize(PullErrorSampleSize);
        }
        THROW_ERROR_EXCEPTION("Could not pull sensors from any endpoint")
            << TErrorAttribute("sampled_error_count", pullErrors.size())
            << TErrorAttribute("total_error_count", totalErrorCount)
            << pullErrors;
    }

    // TODO(achulkov2): Maybe offload this to a separate thread pool like it is done in the exporter.
    outputEncodingContext.Encoder->Close();

    rsp->SetStatus(EStatusCode::OK);
    FillResponseHeaders(outputEncodingContext, rsp->GetHeaders());

    auto replyBlob = TSharedRef::FromString(outputEncodingContext.EncoderBuffer->Str());

    WaitFor(rsp->WriteBody(replyBlob))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
