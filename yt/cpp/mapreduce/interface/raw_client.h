#pragma once

#include "client_method_options.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRawClient
    : public virtual TThrRefBase
{
public:
    // Cypress

    virtual void Set(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
