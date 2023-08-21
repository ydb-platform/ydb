#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/table_client/row_batch.h>

#include <library/cpp/yt/memory/ref.h>
#include <yt/yt/core/misc/range.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IRowStreamEncoder
    : public virtual TRefCounted
{
    virtual TSharedRef Encode(
        const NTableClient::IUnversionedRowBatchPtr& batch,
        const NProto::TRowsetStatistics* statistics) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowStreamEncoder)

////////////////////////////////////////////////////////////////////////////////

struct IRowStreamDecoder
    : public virtual TRefCounted
{
    virtual NTableClient::IUnversionedRowBatchPtr Decode(
        const TSharedRef& payloadRef,
        const NProto::TRowsetDescriptor& descriptor) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRowStreamDecoder)

////////////////////////////////////////////////////////////////////////////////

//! A helper for formatting row stream blocks.
/*!
 *  Serializes #descriptor and (if given) #statistics.
 *
 *  \returns a tuple consisting of
 *  1) the whole row stream block
 *  2) the ref where payload (of size #payloadSize) must be placed
 */
std::tuple<TSharedRef, TMutableRef> SerializeRowStreamBlockEnvelope(
    i64 payloadSize,
    const NApi::NRpcProxy::NProto::TRowsetDescriptor& descriptor,
    const NApi::NRpcProxy::NProto::TRowsetStatistics* statistics);

//! A helper for parsing row stream blocks.
/*!
 *  \returns the paylod slice
 */
TSharedRef DeserializeRowStreamBlockEnvelope(
    const TSharedRef& block,
    NApi::NRpcProxy::NProto::TRowsetDescriptor* descriptor,
    NApi::NRpcProxy::NProto::TRowsetStatistics* statistics);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
