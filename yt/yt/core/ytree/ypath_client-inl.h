#ifndef YPATH_CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include ypath_client.h"
// For the sake of sane code completion.
#include "ypath_client.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TTypedRequest>
TFuture<TIntrusivePtr<typename TTypedRequest::TTypedResponse>>
ExecuteVerb(
    const IYPathServicePtr& service,
    const TIntrusivePtr<TTypedRequest>& request,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    using TTypedResponse = typename TTypedRequest::TTypedResponse;

    auto requestMessage = request->Serialize();
    return ExecuteVerb(service, requestMessage, std::move(logger), logLevel)
        .Apply(BIND([] (const TSharedRefArray& responseMessage) -> TIntrusivePtr<TTypedResponse> {
            auto response = New<TTypedResponse>();
            response->Deserialize(responseMessage);
            return response;
        }));
}

template <class TTypedRequest>
TIntrusivePtr<typename TTypedRequest::TTypedResponse>
SyncExecuteVerb(
    const IYPathServicePtr& service,
    const TIntrusivePtr<TTypedRequest>& request,
    NLogging::TLogger logger,
    NLogging::ELogLevel logLevel)
{
    return ExecuteVerb(service, request, std::move(logger), logLevel)
        .Get()
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
