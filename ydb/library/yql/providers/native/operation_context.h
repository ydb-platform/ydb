#pragma once

#include <library/cpp/threading/cancellation/cancellation_token.h>
#include <util/datetime/base.h>

namespace NYql::NNative {

struct TOperationContext {
    // A provider-local absolute deadline, shared by all phases and retries.
    // Propagation of the client query deadline is a separate integration step.
    TInstant Deadline = TInstant::Max();
    NThreading::TCancellationToken Cancellation = NThreading::TCancellationToken::Default();
};

} // namespace NYql::NNative
