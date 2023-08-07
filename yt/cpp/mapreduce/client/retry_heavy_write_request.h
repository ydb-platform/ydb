#pragma once

#include <yt/cpp/mapreduce/common/fwd.h>

#include <yt/cpp/mapreduce/http/requests.h>

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

void RetryHeavyWriteRequest(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const ITransactionPingerPtr& transactionPinger,
    const TClientContext& context,
    const TTransactionId& parentId,
    THttpHeader& header,
    std::function<THolder<IInputStream>()> streamMaker);

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
