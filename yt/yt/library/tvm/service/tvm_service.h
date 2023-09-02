#pragma once

#include "public.h"

#include <yt/yt/library/tvm/public.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ITvmService
    : public virtual TRefCounted
{
    //! Our TVM id.
    virtual TTvmId GetSelfTvmId() = 0;

    //! Get TVM service ticket from us to serviceAlias. Service mapping must be in config.
    //! Throws on failure.
    virtual TString GetServiceTicket(const TString& serviceAlias) = 0;

    //! Get TVM service ticket from us to serviceId. Service ID must be known (either during
    //! construction or explicitly added in dynamic service).
    //! Throws on failure.
    virtual TString GetServiceTicket(TTvmId serviceId) = 0;

    //! Decode user ticket contents. Throws on failure.
    virtual TParsedTicket ParseUserTicket(const TString& ticket) = 0;

    //! Decode service ticket contents. Throws on failure.
    virtual TParsedServiceTicket ParseServiceTicket(const TString& ticket) = 0;
};

struct IDynamicTvmService
    : public virtual ITvmService
{
public:
    //! Add destination service IDs to fetch. It is possible to add the same ID multiple
    //! times, though it will be added only once really.
    virtual void AddDestinationServiceIds(const std::vector<TTvmId>& serviceIds) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITvmService)
DEFINE_REFCOUNTED_TYPE(IDynamicTvmService)

////////////////////////////////////////////////////////////////////////////////

ITvmServicePtr CreateTvmService(
    TTvmServiceConfigPtr config,
    NProfiling::TProfiler profiler = {});

IDynamicTvmServicePtr CreateDynamicTvmService(
    TTvmServiceConfigPtr config,
    NProfiling::TProfiler profiler = {});

////////////////////////////////////////////////////////////////////////////////

IServiceTicketAuthPtr CreateServiceTicketAuth(
    ITvmServicePtr tvmService,
    TTvmId dstServiceId);

IServiceTicketAuthPtr CreateServiceTicketAuth(
    ITvmServicePtr tvmService,
    TString dstServiceAlias);

////////////////////////////////////////////////////////////////////////////////

TStringBuf RemoveTicketSignature(TStringBuf ticketBody);

////////////////////////////////////////////////////////////////////////////////

bool IsDummyTvmServiceImplementation();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
