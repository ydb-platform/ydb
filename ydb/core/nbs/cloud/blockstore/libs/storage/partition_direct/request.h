#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>

#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentBufferWriteMeta
{
    ui8 Index = 0;
    ui64 Lsn = 0;
};

struct TSyncRequest
{
    ui64 StartIndex;
    ui64 Lsn;
};

struct TRestoreMeta
{
    ui64 BlockIndex = 0;
    ui64 PersistBufferIndex = 0;
    ui64 Lsn = 0;
};

struct TDBGReadBlocksResponse
{
    NProto::TError Error;
};

struct TDBGWriteBlocksResponse
{
    TVector<TPersistentBufferWriteMeta> Meta;
    NProto::TError Error;
};

struct TDBGSyncBlocksResponse
{
    NProto::TError Error;
};

////////////////////////////////////////////////////////////////////////////////

class TBaseRequestHandler
{
private:
    NActors::TActorSystem* const ActorSystem = nullptr;
    ui32 VChunkIndex;

public:
    TBaseRequestHandler(NActors::TActorSystem* actorSystem, ui32 vChunkIndex);

    virtual ~TBaseRequestHandler() = default;

    [[nodiscard]] NActors::TActorSystem* GetActorSystem() const;

    void ChildSpanEndOk(ui64 childRequestId);

    void ChildSpanEndError(ui64 childRequestId, const TString& errorMessage);

    [[nodiscard]] ui32 GetVChunkIndex() const;

    NWilson::TSpan Span;
    std::unordered_map<ui64, NWilson::TSpan> ChildSpanByRequestId;
};

class TIORequestsHandler: public TBaseRequestHandler
{
private:
    const TBlockRange64 Range;

public:
    TIORequestsHandler(
        NActors::TActorSystem* actorSystem,
        ui32 vChunkIndex,
        TBlockRange64 range);

    [[nodiscard]] ui64 GetStartIndex() const;
    [[nodiscard]] ui64 GetStartOffset() const;
    [[nodiscard]] ui64 GetSize() const;
};

////////////////////////////////////////////////////////////////////////////////

class TWriteRequestHandler: public TIORequestsHandler
{
public:
    TWriteRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui32 vChunkIndex,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TWriteRequestHandler() override = default;

    NWilson::TSpan& GetChildSpan(ui64 requestId, ui8 persistentBufferIndex);

    void OnWriteRequested(ui64 requestId, ui8 persistentBufferIndex, ui64 lsn);
    void OnWriteFinished(
        ui64 requestId,
        const NKikimrBlobStorage::NDDisk::TEvWritePersistentBufferResult&
            result);

    [[nodiscard]] TGuardedSgList GetData();

    NThreading::TFuture<TDBGWriteBlocksResponse> GetFuture();
    void SetResponse(NProto::TError error);

private:
    void SetCompleted(ui64 requestId);
    [[nodiscard]] bool IsCompleted() const;
    [[nodiscard]] TVector<TPersistentBufferWriteMeta> GetWritesMeta() const;

    TSpinLock Lock;
    std::shared_ptr<TWriteBlocksLocalRequest> Request;
    NThreading::TPromise<TDBGWriteBlocksResponse> Promise =
        NThreading::NewPromise<TDBGWriteBlocksResponse>();
    const ui8 RequiredAckCount = 3;
    ui8 AckCount = 0;
    ui8 AcksMask = 0;
    std::unordered_map<ui64, TPersistentBufferWriteMeta> WriteMetaByRequestId;
};

class TSyncAndEraseRequestHandler: public TBaseRequestHandler
{
public:
    TSyncAndEraseRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui32 vChunkIndex,
        ui8 persistentBufferIndex,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    TSyncAndEraseRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui32 vChunkIndex,
        ui8 persistentBufferIndex,
        NWilson::TTraceId traceId,
        ui64 tabletId,
        TVector<TSyncRequest> syncRequests);

    ~TSyncAndEraseRequestHandler() override = default;

    NWilson::TSpan& GetChildSpan(ui64 requestId);

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

    [[nodiscard]] ui64 OnSyncRequested(ui64 startIndex, ui64 lsn);

    [[nodiscard]] const TVector<TSyncRequest>& GetSyncRequests() const;

    [[nodiscard]] TVector<NKikimr::NDDisk::TBlockSelector>
    GetBlockSelectors() const;
    [[nodiscard]] TVector<ui64> GetLsns() const;

    NThreading::TFuture<TDBGSyncBlocksResponse> GetFuture();
    void SetResponse(NProto::TError error);

private:
    NThreading::TPromise<TDBGSyncBlocksResponse> Promise =
        NThreading::NewPromise<TDBGSyncBlocksResponse>();
    ui8 PersistentBufferIndex;
    TVector<TSyncRequest> SyncRequests;
};

////////////////////////////////////////////////////////////////////////////////

class TReadRequestHandler: public TIORequestsHandler
{
public:
    TReadRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui32 vChunkIndex,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TReadRequestHandler() override = default;

    NWilson::TSpan& GetChildSpan(ui64 requestId, bool isReadPersistentBuffer);

    [[nodiscard]] TGuardedSgList GetData();

    NThreading::TFuture<TDBGReadBlocksResponse> GetFuture();
    void SetResponse(NProto::TError error);

private:
    std::shared_ptr<TReadBlocksLocalRequest> Request;
    NThreading::TPromise<TDBGReadBlocksResponse> Promise =
        NThreading::NewPromise<TDBGReadBlocksResponse>();
};

class TOverallAckRequestHandler: public TBaseRequestHandler
{
public:
    TOverallAckRequestHandler(
        NActors::TActorSystem* actorSystem,
        NWilson::TTraceId traceId,
        TString name,
        ui64 tabletId,
        ui32 vChunkIndex,
        ui8 requiredAckCount);

    ~TOverallAckRequestHandler() override = default;

    NWilson::TSpan GetChildSpan(ui64 requestId, TString eventName);

    [[nodiscard]] bool IsCompleted() const;

    void RegisterCompetedRequest()
    {
        ++AckCount;
    }

    [[nodiscard]] ui8 GetRequiredAckCount() const
    {
        return RequiredAckCount;
    }

    [[nodiscard]] NThreading::TFuture<void> GetFuture() const
    {
        return Promise.GetFuture();
    }

    void SetResponse()
    {
        Promise.SetValue();
    }

private:
    const ui8 RequiredAckCount;
    ui8 AckCount = 0;
    TString Name;
    NThreading::TPromise<void> Promise;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
