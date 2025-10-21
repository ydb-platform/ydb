#pragma once

#include "prerequisite.h"
#include "prerequisite_client.h"

#include <yt/yt/client/chaos_client/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TChaosLeaseStartOptions
    : public TPrerequisiteAttachOptions
{
    std::optional<TDuration> LeaseTimeout;

    NChaosClient::TChaosLeaseId ParentId;

    NYTree::IAttributeDictionaryPtr Attributes;
};

////////////////////////////////////////////////////////////////////////////////

struct TChaosLeaseAttachOptions
    : public TPrerequisiteAttachOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct IChaosClient
{
    virtual ~IChaosClient() = default;

    virtual TFuture<IPrerequisitePtr> StartChaosLease(const TChaosLeaseStartOptions& options = {}) = 0;

    virtual TFuture<IPrerequisitePtr> AttachChaosLease(
        NChaosClient::TChaosLeaseId chaosLeaseId,
        const TChaosLeaseAttachOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
