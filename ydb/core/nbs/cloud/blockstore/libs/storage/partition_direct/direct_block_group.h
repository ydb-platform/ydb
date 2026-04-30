#pragma once

#include "restore_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/guarded_sglist.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/scheduler.h>
#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/public.h>

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <ydb/library/actors/wilson/wilson_span.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TDBGReadBlocksResponse
{
    NProto::TError Error;
};

struct TDBGWriteBlocksResponse
{
    NProto::TError Error;
};

struct TDBGWriteBlocksToManyPBuffersResponse
{
    struct TSinglePersistentBufferResult
    {
        THostIndex HostIndex = InvalidHostIndex;
        NProto::TError Error;
    };

    static TDBGWriteBlocksToManyPBuffersResponse MakeOverallError(
        EWellKnownResultCodes code,
        TString reason);

    TVector<TSinglePersistentBufferResult> Responses;
    NProto::TError OverallError;
};

struct TDBGFlushResponse
{
    TVector<NProto::TError> Errors;
};

struct TDBGEraseResponse
{
    NProto::TError Error;
};

struct TDBGRestoreResponse
{
    struct TRestoreMeta
    {
        ui64 Lsn = 0;
        TBlockRange64 Range;
        THostIndex HostIndex = InvalidHostIndex;
    };

    NProto::TError Error;
    TVector<TRestoreMeta> Meta;
};

struct TListPBufferMeta
{
    ui32 VChunkIndex = 0;
    ui64 Lsn = 0;
    TBlockRange64 Range;
};

using TListPBufferMetaVector = TVector<TListPBufferMeta>;

struct TListPBufferResponse
{
    NProto::TError Error;
    TListPBufferMetaVector Meta;
};

struct TAggregatedListPBufferResponse
{
    NProto::TError Error;
    TMap<THostIndex, TListPBufferMetaVector> Meta;
};

struct TDDiskIdLess
{
    using TDDiskId = NKikimrBlobStorage::NDDisk::TDDiskId;
    bool operator()(const TDDiskId& lhs, const TDDiskId& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

// Abstract base interface for DirectBlockGroup implementations
class IDirectBlockGroup
{
public:
    virtual ~IDirectBlockGroup() = default;

    virtual TExecutorPtr GetExecutor() = 0;

    virtual void Schedule(TDuration delay, TCallback callback) = 0;

    virtual std::shared_ptr<NWilson::TSpan> CreateChildSpan(
        const NWilson::TTraceId& traceId,
        TStringBuf name) = 0;

    virtual void EstablishConnections() = 0;

    virtual NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromDDisk(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    virtual NThreading::TFuture<TDBGReadBlocksResponse> ReadBlocksFromPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    virtual NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToDDisk(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    virtual NThreading::TFuture<TDBGWriteBlocksResponse> WriteBlocksToPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        ui64 lsn,
        TBlockRange64 range,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    virtual NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>
    WriteBlocksToManyPBuffers(
        ui32 vChunkIndex,
        TVector<THostIndex> hostIndexes,
        ui64 lsn,
        TBlockRange64 range,
        TDuration replyTimeout,
        const TGuardedSgList& guardedSglist,
        const NWilson::TTraceId& traceId) = 0;

    // Batch operation to flush a list of PBuffer entries. It can be executed in
    // two modes - when the source and destination are the same host, and when
    // the source and destination hosts are different. In this case, the
    // operation is performed in pull mode, when the destination host downloads
    // entries from PBuffer and write it to DDisk to self.
    virtual NThreading::TFuture<TDBGFlushResponse> SyncWithPBuffer(
        ui32 vChunkIndex,
        THostIndex pbufferHostIndex,   // source host
        THostIndex ddiskHostIndex,     // destination host
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) = 0;

    // Batch operation to erase a list of PBuffer entries.
    virtual NThreading::TFuture<TDBGEraseResponse> EraseFromPBuffer(
        ui32 vChunkIndex,
        THostIndex hostIndex,
        const TVector<TPBufferSegment>& segments,
        const NWilson::TTraceId& traceId) = 0;

    // Get a list of all entries in PBuffers belonging to a given vChunkIndex.
    virtual NThreading::TFuture<TDBGRestoreResponse> RestoreDBGPBuffers(
        ui32 vChunkIndex) = 0;

    // Query persistent buffer from Node.
    virtual NThreading::TFuture<TListPBufferResponse> ListPBuffers(
        THostIndex hostIndex) = 0;
};

using IDirectBlockGroupPtr = std::shared_ptr<IDirectBlockGroup>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
