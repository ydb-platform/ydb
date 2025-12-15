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

struct TTransferBundleResourcesOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IAccountingClient
{
    virtual ~IAccountingClient() = default;

    virtual TFuture<void> TransferAccountResources(
        const std::string& srcAccount,
        const std::string& dstAccount,
        NYTree::INodePtr resourceDelta,
        const TTransferAccountResourcesOptions& options = {}) = 0;

    virtual TFuture<void> TransferPoolResources(
        const std::string& srcPool,
        const std::string& dstPool,
        const std::string& poolTree,
        NYTree::INodePtr resourceDelta,
        const TTransferPoolResourcesOptions& options = {}) = 0;

    virtual TFuture<void> TransferBundleResources(
        const std::string& srcBundle,
        const std::string& dstBundle,
        NYTree::INodePtr resourceDelta,
        const TTransferBundleResourcesOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

