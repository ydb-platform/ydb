#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/rpc/public.h>
#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/service_discovery/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error);
bool IsChannelFailureError(const TError& error);

bool IsChannelFailureErrorHandled(const TError& error);
void LabelHandledChannelFailureError(TError* error);

//! Returns a wrapper that sets the timeout for every request (unless it is given
//! explicitly in the request itself).
IChannelPtr CreateDefaultTimeoutChannel(
    IChannelPtr underlyingChannel,
    TDuration timeout);
IChannelFactoryPtr CreateDefaultTimeoutChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    TDuration timeout);

//! Returns a wrapper that sets "authenticated_user" attribute in every request.
IChannelPtr CreateAuthenticatedChannel(
    IChannelPtr underlyingChannel,
    TAuthenticationIdentity identity);
IChannelFactoryPtr CreateAuthenticatedChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    TAuthenticationIdentity identity);

//! Returns a wrapper that sets realm id in every request.
IChannelPtr CreateRealmChannel(
    IChannelPtr underlyingChannel,
    TRealmId realmId);
IChannelFactoryPtr CreateRealmChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    TRealmId realmId);

//! Returns a wrapper that informs about channel failures.
/*!
 *  Channel failures are being detected via provided filter.
 */
IChannelPtr CreateFailureDetectingChannel(
    IChannelPtr underlyingChannel,
    std::optional<TDuration> acknowledgementTimeout,
    TCallback<void(const IChannelPtr&, const TError& error)> onFailure,
    TCallback<bool(const TError&)> isError = BIND(IsChannelFailureError),
    TCallback<TError(TError)> maybeTransformError = {});

NTracing::TTraceContextPtr GetOrCreateHandlerTraceContext(
    const NProto::TRequestHeader& header,
    bool forceTracing);
NTracing::TTraceContextPtr CreateCallTraceContext(
    std::string service,
    std::string method);

//! Generates a random mutation id.
TMutationId GenerateMutationId();
//! Enables generating a series of mutation ids within a batch.
TMutationId GenerateNextBatchMutationId(TMutationId id);
//! Enables generating a series of mutation ids within a forwarding chain.
TMutationId GenerateNextForwardedMutationId(TMutationId id);

void GenerateMutationId(const IClientRequestPtr& request);
TMutationId GetMutationId(const NProto::TRequestHeader& header);
void SetMutationId(NProto::TRequestHeader* header, TMutationId id, bool retry);
void SetMutationId(const IClientRequestPtr& request, TMutationId id, bool retry);
void SetOrGenerateMutationId(const IClientRequestPtr& request, TMutationId id, bool retry);

void SetAuthenticationIdentity(const IClientRequestPtr& request, const TAuthenticationIdentity& identity);
void SetCurrentAuthenticationIdentity(const IClientRequestPtr& request);

template <class T>
void WriteAuthenticationIdentityToProto(T* proto, const TAuthenticationIdentity& identity);
template <class T>
TAuthenticationIdentity ParseAuthenticationIdentityFromProto(const T& proto);

std::vector<TString> AddressesFromEndpointSet(const NServiceDiscovery::TEndpointSet& endpointSet);

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<TSharedRef>> AsyncCompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId);

TFuture<std::vector<TSharedRef>> AsyncDecompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId);

std::vector<TSharedRef> CompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId);

std::vector<TSharedRef> DecompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId);

////////////////////////////////////////////////////////////////////////////////

template <class E>
int FeatureIdToInt(E featureId);

void EnrichClientRequestError(
    TError* error,
    TFeatureIdFormatter featureIdFormatter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
