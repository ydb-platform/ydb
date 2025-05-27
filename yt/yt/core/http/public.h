#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THeaders)

DECLARE_REFCOUNTED_STRUCT(IRequest)
DECLARE_REFCOUNTED_STRUCT(IResponse)
DECLARE_REFCOUNTED_STRUCT(IResponseWriter)
DECLARE_REFCOUNTED_STRUCT(IActiveRequest)

DECLARE_REFCOUNTED_STRUCT(IServer)
DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(IRetryingClient)
DECLARE_REFCOUNTED_STRUCT(IResponseChecker)
DECLARE_REFCOUNTED_STRUCT(IRequestPathMatcher)
DECLARE_REFCOUNTED_STRUCT(IHttpHandler)

DECLARE_REFCOUNTED_STRUCT(THttpIOConfig)
DECLARE_REFCOUNTED_STRUCT(TServerConfig)
DECLARE_REFCOUNTED_STRUCT(TClientConfig)
DECLARE_REFCOUNTED_STRUCT(TRetryingClientConfig)
DECLARE_REFCOUNTED_STRUCT(TCorsConfig)

DECLARE_REFCOUNTED_CLASS(TConnectionPool)

DECLARE_REFCOUNTED_CLASS(TSharedRefOutputStream)

////////////////////////////////////////////////////////////////////////////////

using TContentEncoding = std::string;
using TConnectionId = TGuid;
using TRequestId = TGuid;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
