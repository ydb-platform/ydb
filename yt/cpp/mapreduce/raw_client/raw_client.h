#pragma once

#include <yt/cpp/mapreduce/http/context.h>

#include <yt/cpp/mapreduce/interface/client_method_options.h>
#include <yt/cpp/mapreduce/interface/raw_client.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

class THttpRawClient
    : public IRawClient
{
public:
    THttpRawClient(const TClientContext& context);

    // Cypress

    TNode Get(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options = {}) override;

    TNode TryGet(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TGetOptions& options) override;

    void Set(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode& value,
        const TSetOptions& options = {}) override;

    bool Exists(
        TMutationId& mutataionId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TExistsOptions& options = {}) override;

    void MultisetAttributes(
        TMutationId& mutationId,
        const TTransactionId& transactionId,
        const TYPath& path,
        const TNode::TMapType& value,
        const TMultisetAttributesOptions& options = {}) override;

private:
    const TClientContext Context_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
