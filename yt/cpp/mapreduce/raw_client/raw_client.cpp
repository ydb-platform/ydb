#include "raw_client.h"

#include "rpc_parameters_serialization.h"

#include <yt/cpp/mapreduce/http/http.h>
#include <yt/cpp/mapreduce/http/requests.h>
#include <yt/cpp/mapreduce/http/retry_request.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

THttpRawClient::THttpRawClient(const TClientContext& context)
    : Context_(context)
{ }

TNode THttpRawClient::Get(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    THttpHeader header("GET", "get");
    header.MergeParameters(NRawClient::SerializeParamsForGet(transactionId, Context_.Config->Prefix, path, options));
    return NodeFromYsonString(RequestWithoutRetry(Context_, mutationId, header).Response);
}

TNode THttpRawClient::TryGet(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    try {
        return Get(mutationId, transactionId, path, options);
    } catch (const TErrorResponse& error) {
        if (!error.IsResolveError()) {
            throw;
        }
        return TNode();
    }
}

void THttpRawClient::Set(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    THttpHeader header("PUT", "set");
    header.MergeParameters(NRawClient::SerializeParamsForSet(transactionId, Context_.Config->Prefix, path, options));
    auto body = NodeToYsonString(value);
    RequestWithoutRetry(Context_, mutationId, header, body);
}

bool THttpRawClient::Exists(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TExistsOptions& options)
{
    THttpHeader header("GET", "exists");
    header.MergeParameters(NRawClient::SerializeParamsForExists(transactionId, Context_.Config->Prefix, path, options));
    return ParseBoolFromResponse(RequestWithoutRetry(Context_, mutationId, header).Response);
}

void THttpRawClient::MultisetAttributes(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TNode::TMapType& value,
    const TMultisetAttributesOptions& options)
{
    THttpHeader header("PUT", "api/v4/multiset_attributes", false);
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForMultisetAttributes(transactionId, Context_.Config->Prefix, path, options));
    auto body = NodeToYsonString(value);
    RequestWithoutRetry(Context_, mutationId, header, body);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
