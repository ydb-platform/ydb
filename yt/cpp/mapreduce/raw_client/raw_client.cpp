#include "raw_client.h"

#include "rpc_parameters_serialization.h"

#include <yt/cpp/mapreduce/common/helpers.h>

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
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    TMutationId mutationId;
    THttpHeader header("GET", "get");
    header.MergeParameters(NRawClient::SerializeParamsForGet(transactionId, Context_.Config->Prefix, path, options));
    return NodeFromYsonString(RequestWithoutRetry(Context_, mutationId, header).Response);
}

TNode THttpRawClient::TryGet(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TGetOptions& options)
{
    try {
        return Get(transactionId, path, options);
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
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForSet(transactionId, Context_.Config->Prefix, path, options));
    auto body = NodeToYsonString(value);
    RequestWithoutRetry(Context_, mutationId, header, body);
}

bool THttpRawClient::Exists(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TExistsOptions& options)
{
    TMutationId mutationId;
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

TNodeId THttpRawClient::Create(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const ENodeType& type,
    const TCreateOptions& options)
{
    THttpHeader header("POST", "create");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForCreate(transactionId, Context_.Config->Prefix, path, type, options));
    return ParseGuidFromResponse(RequestWithoutRetry(Context_, mutationId, header).Response);
}


TNodeId THttpRawClient::CopyWithoutRetries(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    TMutationId mutationId;
    THttpHeader header("POST", "copy");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForCopy(transactionId, Context_.Config->Prefix, sourcePath, destinationPath, options));
    return ParseGuidFromResponse(RequestWithoutRetry(Context_, mutationId, header).Response);
}

TNodeId THttpRawClient::CopyInsideMasterCell(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    THttpHeader header("POST", "copy");
    header.AddMutationId();
    auto params = NRawClient::SerializeParamsForCopy(transactionId, Context_.Config->Prefix, sourcePath, destinationPath, options);

    // Make cross cell copying disable.
    params["enable_cross_cell_copying"] = false;
    header.MergeParameters(params);
    return ParseGuidFromResponse(RequestWithoutRetry(Context_, mutationId, header).Response);
}

TNodeId THttpRawClient::MoveWithoutRetries(
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    TMutationId mutationId;
    THttpHeader header("POST", "move");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForMove(transactionId, Context_.Config->Prefix, sourcePath, destinationPath, options));
    return ParseGuidFromResponse(RequestWithoutRetry(Context_, mutationId, header).Response);
}

TNodeId THttpRawClient::MoveInsideMasterCell(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    THttpHeader header("POST", "move");
    header.AddMutationId();
    auto params = NRawClient::SerializeParamsForMove(transactionId, Context_.Config->Prefix, sourcePath, destinationPath, options);

    // Make cross cell copying disable.
    params["enable_cross_cell_copying"] = false;
    header.MergeParameters(params);
    return ParseGuidFromResponse(RequestWithoutRetry(Context_, mutationId, header).Response);
}

void THttpRawClient::Remove(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TRemoveOptions& options)
{
    THttpHeader header("POST", "remove");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForRemove(transactionId, Context_.Config->Prefix, path, options));
    RequestWithoutRetry(Context_, mutationId, header);
}

TNode::TListType THttpRawClient::List(
    const TTransactionId& transactionId,
    const TYPath& path,
    const TListOptions& options)
{
    TMutationId mutationId;
    THttpHeader header("GET", "list");

    TYPath updatedPath = AddPathPrefix(path, Context_.Config->Prefix);
    // Translate "//" to "/"
    // Translate "//some/constom/prefix/from/config/" to "//some/constom/prefix/from/config"
    if (path.empty() && updatedPath.EndsWith('/')) {
        updatedPath.pop_back();
    }
    header.MergeParameters(NRawClient::SerializeParamsForList(transactionId, Context_.Config->Prefix, updatedPath, options));
    auto result = RequestWithoutRetry(Context_, mutationId, header);
    return NodeFromYsonString(result.Response).AsList();
}

TNodeId THttpRawClient::Link(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    THttpHeader header("POST", "link");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForLink(transactionId, Context_.Config->Prefix, targetPath, linkPath, options));
    return ParseGuidFromResponse(RequestWithoutRetry(Context_, mutationId, header).Response);
}

TLockId THttpRawClient::Lock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    THttpHeader header("POST", "lock");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForLock(transactionId, Context_.Config->Prefix, path, mode, options));
    return ParseGuidFromResponse(RequestWithoutRetry(Context_, mutationId, header).Response);
}

void THttpRawClient::Unlock(
    TMutationId& mutationId,
    const TTransactionId& transactionId,
    const TYPath& path,
    const TUnlockOptions& options)
{
    THttpHeader header("POST", "unlock");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForUnlock(transactionId, Context_.Config->Prefix, path, options));
    RequestWithoutRetry(Context_, mutationId, header);
}

void THttpRawClient::Concatenate(
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& sourcePaths,
    const TRichYPath& destinationPath,
    const TConcatenateOptions& options)
{
    TMutationId mutationId;
    THttpHeader header("POST", "concatenate");
    header.AddMutationId();
    header.MergeParameters(NRawClient::SerializeParamsForConcatenate(transactionId, Context_.Config->Prefix, sourcePaths, destinationPath, options));
    RequestWithoutRetry(Context_, mutationId, header);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
