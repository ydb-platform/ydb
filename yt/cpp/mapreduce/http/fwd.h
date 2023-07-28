#pragma once

#include <memory>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TClientContext;
class THttpHeader;

namespace NHttpClient {

class IHttpClient;
class IHttpRequest;
class IHttpResponse;

using IHttpClientPtr = std::shared_ptr<IHttpClient>;
using IHttpResponsePtr = std::unique_ptr<IHttpResponse>;
using IHttpRequestPtr = std::unique_ptr<IHttpRequest>;

} // namespace NHttpClient

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
