#include "iproducer_p.h"
#include "persqueue_p.h"

namespace NPersQueue {

TPublicProducer::TPublicProducer(std::shared_ptr<IProducerImpl> impl)
    : Impl(std::move(impl))
{
}

TPublicProducer::~TPublicProducer() {
    Impl->Cancel();
}

IProducerImpl::IProducerImpl(std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib)
    : TSyncDestroyed(std::move(destroyEventRef), std::move(pqLib))
{
}

} // namespace NPersQueue
