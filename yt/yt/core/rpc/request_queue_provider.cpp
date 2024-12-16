#include "request_queue_provider.h"

#include "service_detail.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

void TRequestQueueProviderBase::ConfigureQueue(
    TRequestQueue* queue,
    const TMethodConfigPtr& config)
{
    if (config) {
        queue->Configure(config);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
