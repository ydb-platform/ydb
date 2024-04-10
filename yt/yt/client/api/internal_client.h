#pragma once

#include "client.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/library/erasure/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct THunkDescriptor
{
    NChunkClient::TChunkId ChunkId;
    NErasure::ECodec ErasureCodec = NErasure::ECodec::None;
    int BlockIndex = -1;
    i64 BlockOffset = -1;
    i64 Length = -1;
    std::optional<i64> BlockSize;
};

////////////////////////////////////////////////////////////////////////////////

class TSerializableHunkDescriptor
    : public THunkDescriptor
    , public NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TSerializableHunkDescriptor);

    static void Register(TRegistrar registrar);
};

using TSerializableHunkDescriptorPtr = TIntrusivePtr<TSerializableHunkDescriptor>;

TSerializableHunkDescriptorPtr CreateSerializableHunkDescriptor(const THunkDescriptor& descriptor);

////////////////////////////////////////////////////////////////////////////////

struct TReadHunksOptions
    : public TTimeoutOptions
{
    NChunkClient::TChunkFragmentReaderConfigPtr Config;

    //! If false, chunk fragment is returned as is.
    bool ParseHeader;
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteHunksOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TLockHunkStoreOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TUnlockHunkStoreOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TIssueLeaseOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TRevokeLeaseOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TReferenceLeaseOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TUnreferenceLeaseOptions
    : public TTimeoutOptions
{ };

////////////////////////////////////////////////////////////////////////////////

struct TGetOrderedTabletSafeTrimRowCountOptions
    : public TTimeoutOptions
{ };

struct TGetOrderedTabletSafeTrimRowCountRequest
{
    NYTree::TYPath Path;
    int TabletIndex;
    NTransactionClient::TTimestamp Timestamp;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a set of private APIs.
/*!
 *  Only native clients are expected to implement this.
 */
struct IInternalClient
    : public virtual TRefCounted
{
    virtual TFuture<std::vector<TSharedRef>> ReadHunks(
        const std::vector<THunkDescriptor>& descriptors,
        const TReadHunksOptions& options = {}) = 0;

    virtual TFuture<std::vector<THunkDescriptor>> WriteHunks(
        const NYTree::TYPath& path,
        int tabletIndex,
        const std::vector<TSharedRef>& payloads,
        const TWriteHunksOptions& options = {}) = 0;

    virtual TFuture<void> LockHunkStore(
        const NYTree::TYPath& path,
        int tabletIndex,
        NTabletClient::TStoreId storeId,
        NTabletClient::TTabletId lockerTabletId,
        const TLockHunkStoreOptions& options = {}) = 0;
    virtual TFuture<void> UnlockHunkStore(
        const NYTree::TYPath& path,
        int tabletIndex,
        NTabletClient::TStoreId storeId,
        NTabletClient::TTabletId lockerTabletId,
        const TUnlockHunkStoreOptions& options = {}) = 0;

    //! Same as NApi::IClient::PullQueue, but without authentication.
    //! This is used inside methods like NApi::IClient::PullConsumer, which perform their own authentication
    //! and allow reading from a queue without having read permissions for the underlying dynamic table.
    virtual TFuture<NQueueClient::IQueueRowsetPtr> PullQueueUnauthenticated(
        const NYPath::TRichYPath& queuePath,
        i64 offset,
        int partitionIndex,
        const NQueueClient::TQueueRowBatchReadOptions& rowBatchReadOptions,
        const TPullQueueOptions& options = {}) = 0;

    //! For each request, finds and returns some index bounds in the given ordered tablet based on the given timestamp.
    //! See response definition for details.
    virtual TFuture<std::vector<TErrorOr<i64>>> GetOrderedTabletSafeTrimRowCount(
        const std::vector<TGetOrderedTabletSafeTrimRowCountRequest>& requests,
        const TGetOrderedTabletSafeTrimRowCountOptions& options = {}) = 0;

    virtual TFuture<void> IssueLease(
        NHydra::TCellId cellId,
        NObjectClient::TObjectId leaseId,
        const TIssueLeaseOptions& options = {}) = 0;
    virtual TFuture<void> RevokeLease(
        NHydra::TCellId cellId,
        NObjectClient::TObjectId leaseId,
        bool force,
        const TRevokeLeaseOptions& options = {}) = 0;

    virtual TFuture<void> ReferenceLease(
        NHydra::TCellId cellId,
        NObjectClient::TObjectId leaseId,
        bool persistent,
        bool force,
        const TReferenceLeaseOptions& options = {}) = 0;
    virtual TFuture<void> UnreferenceLease(
        NHydra::TCellId cellId,
        NObjectClient::TObjectId leaseId,
        bool persistent,
        const TUnreferenceLeaseOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IInternalClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
