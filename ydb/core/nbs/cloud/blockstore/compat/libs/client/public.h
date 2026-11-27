#pragma once

#include <memory>

namespace NCloud::NBlockStore {

namespace NProto {
class TClientConfig;
class TClientPerformanceProfile;
class TClientProfile;
}   // namespace NProto

namespace NClient {

////////////////////////////////////////////////////////////////////////////////

class TClientAppConfig;
using TClientAppConfigPtr = std::shared_ptr<TClientAppConfig>;

struct IThrottlerProvider;
using IThrottlerProviderPtr = std::shared_ptr<IThrottlerProvider>;

struct IClient;
using IClientPtr = std::shared_ptr<IClient>;

struct IMultiHostClient;
using IMultiHostClientPtr = std::shared_ptr<IMultiHostClient>;

struct IRetryPolicy;
using IRetryPolicyPtr = std::shared_ptr<IRetryPolicy>;

struct ISession;
using ISessionPtr = std::shared_ptr<ISession>;

struct ISwitchableSession;
using ISwitchableSessionPtr = std::shared_ptr<ISwitchableSession>;

struct TSessionConfig;

struct IMetricClient;
using IMetricClientPtr = std::shared_ptr<IMetricClient>;

struct IMetricDataClient;
using IMetricDataClientPtr = std::shared_ptr<IMetricDataClient>;

struct IMultiClientEndpoint;
using IMultiClientEndpointPtr = std::shared_ptr<IMultiClientEndpoint>;

}   // namespace NClient
}   // namespace NCloud::NBlockStore
