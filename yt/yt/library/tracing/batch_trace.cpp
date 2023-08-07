#include "batch_trace.h"

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

void TBatchTrace::Join()
{
    if (auto* context = TryGetCurrentTraceContext(); context && context->IsRecorded()) {
        Join(MakeStrong(context));
    }
}

void TBatchTrace::Join(const TTraceContextPtr& context)
{
    if (!context->IsRecorded()) {
        return;
    }

    Clients_.push_back(context);
}

std::pair<TTraceContextPtr, bool> TBatchTrace::StartSpan(const TString& spanName)
{
    auto traceContext = TTraceContext::NewRoot(spanName);

    bool hasBlockedClient = false;
    for (const auto& client : Clients_) {
        if (client->AddAsyncChild(traceContext->GetTraceId())) {
            hasBlockedClient = true;
        }
    }
    Clients_.clear();

    if (hasBlockedClient) {
        traceContext->SetSampled();
    }

    return {traceContext, hasBlockedClient};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
