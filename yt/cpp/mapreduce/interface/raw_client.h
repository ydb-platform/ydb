#pragma once

#include "client_method_options.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class IRawClient
    : public virtual TThrRefBase
{
public:
    // Cypress

    virtual TNode Get(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {}) = 0;

    virtual TNode TryGet(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {}) = 0;

    virtual void Set(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {}) = 0;

    virtual bool Exists(
        TMutationId& mutataionId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TExistsOptions& options = {}) = 0;

    virtual void MultisetAttributes(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode::TMapType& value,
        const TMultisetAttributesOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
