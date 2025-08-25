#include "per_key_request_queue_provider.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

TPerUserRequestQueueProvider::TPerUserRequestQueueProvider(
    TReconfigurationCallback reconfigurationCallback,
    NProfiling::TProfiler throttlersProfiler)
    : TBase(
        CreateKeyFromRequestHeaderCallback(),
        std::move(reconfigurationCallback))
    , ThrottlersProfiler_(std::move(throttlersProfiler))
{ }

TRequestQueuePtr TPerUserRequestQueueProvider::CreateQueueForKey(const std::string& userName)
{
    return CreateRequestQueue(userName, ThrottlersProfiler_.WithTag("user", userName));
}

bool TPerUserRequestQueueProvider::IsReconfigurationPermitted(const std::string& userName) const
{
    return userName != RootUserName;
}

TPerUserRequestQueueProvider::TKeyFromRequestHeaderCallback TPerUserRequestQueueProvider::CreateKeyFromRequestHeaderCallback()
{
    return BIND([] (const NProto::TRequestHeader& header) {
        return header.has_user() ? ::NYT::FromProto<std::string>(header.user()) : RootUserName;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
