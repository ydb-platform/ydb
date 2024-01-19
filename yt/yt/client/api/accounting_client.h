#pragma once

#include "client_common.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TTransferAccountResourcesOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TTransferPoolResourcesOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IAccountingClient
{
    virtual ~IAccountingClient() = default;

    virtual TFuture<void> TransferAccountResources(
        const TString& srcAccount,
        const TString& dstAccount,
        NYTree::INodePtr resourceDelta,
        const TTransferAccountResourcesOptions& options = {}) = 0;

    virtual TFuture<void> TransferPoolResources(
        const TString& srcPool,
        const TString& dstPool,
        const TString& poolTree,
        NYTree::INodePtr resourceDelta,
        const TTransferPoolResourcesOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

