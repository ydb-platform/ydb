#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpc::NGrpc {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TSslPemKeyCertPairConfig)
DECLARE_REFCOUNTED_CLASS(TServerCredentialsConfig)
DECLARE_REFCOUNTED_CLASS(TServerAddressConfig)
DECLARE_REFCOUNTED_CLASS(TServerConfig)
DECLARE_REFCOUNTED_CLASS(TChannelCredentialsConfig)
DECLARE_REFCOUNTED_CLASS(TChannelConfigTemplate)
DECLARE_REFCOUNTED_CLASS(TChannelConfig)

////////////////////////////////////////////////////////////////////////////////

extern const char* const TracingTraceIdMetadataKey;
extern const char* const TracingSpanIdMetadataKey;
extern const char* const TracingSampledMetadataKey;
extern const char* const TracingDebugMetadataKey;

extern const char* const RequestIdMetadataKey;
extern const char* const UserMetadataKey;
extern const char* const UserTagMetadataKey;
extern const char* const UserAgentMetadataKey;
extern const char* const AuthTokenMetadataKey;
extern const char* const AuthSessionIdMetadataKey;
extern const char* const AuthSslSessionIdMetadataKey;
extern const char* const AuthUserTicketMetadataKey;
extern const char* const AuthServiceTicketMetadataKey;
extern const char* const ErrorMetadataKey;
extern const char* const MessageBodySizeMetadataKey;
extern const char* const ProtocolVersionMetadataKey;
extern const char* const RequestCodecKey;
extern const char* const ResponseCodecKey;

// After adding a new metadata key, do not forget to add it in GetNativeMetadataKeys.
const THashSet<TStringBuf>& GetNativeMetadataKeys();

constexpr int GenericErrorStatusCode = 100;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
