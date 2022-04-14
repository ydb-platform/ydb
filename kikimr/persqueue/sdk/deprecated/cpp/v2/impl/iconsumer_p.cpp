#include "iconsumer_p.h"
#include "persqueue_p.h"

namespace NPersQueue {

TPublicConsumer::TPublicConsumer(std::shared_ptr<IConsumerImpl> impl)
    : Impl(std::move(impl))
{
}

TPublicConsumer::~TPublicConsumer() {
    Impl->Cancel();
}

IConsumerImpl::IConsumerImpl(std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib)
    : TSyncDestroyed(std::move(destroyEventRef), std::move(pqLib))
{
}

} // namespace NPersQueue
