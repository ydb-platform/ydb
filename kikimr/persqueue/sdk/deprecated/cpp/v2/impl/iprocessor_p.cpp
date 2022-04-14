#include "iprocessor_p.h"
#include "persqueue_p.h"

namespace NPersQueue {

TPublicProcessor::TPublicProcessor(std::shared_ptr<IProcessorImpl> impl)
    : Impl(std::move(impl))
{
}

TPublicProcessor::~TPublicProcessor() {
    Impl->Cancel();
}

IProcessorImpl::IProcessorImpl(std::shared_ptr<void> destroyEventRef, TIntrusivePtr<TPQLibPrivate> pqLib)
    : TSyncDestroyed(std::move(destroyEventRef), std::move(pqLib))
{
}

} // namespace NPersQueue
