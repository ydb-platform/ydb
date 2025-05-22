#pragma once

#include "transaction.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

class TQueueTransactionMixin
    : public virtual ITransaction
{
public:
    // TODO(nadya73): Remove it: YT-20712
    void AdvanceConsumer(
        const NYPath::TRichYPath& consumerPath,
        const NYPath::TRichYPath& queuePath,
        int partitionIndex,
        std::optional<i64> oldOffset,
        i64 newOffset) override;;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

