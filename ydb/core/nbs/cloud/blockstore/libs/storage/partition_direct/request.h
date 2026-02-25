#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TBaseRequestHandler
{
private:
    NActors::TActorSystem* const ActorSystem = nullptr;
    ui32 VChunkIndex;

public:
    TBaseRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui32 vChunkIndex);

    virtual ~TBaseRequestHandler() = default;

    [[nodiscard]] NActors::TActorSystem* GetActorSystem() const;

    virtual bool IsCompleted(ui64 requestId) = 0;

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

    virtual ~TIORequestsHandler() = default;

    [[nodiscard]] ui64 GetStartIndex() const;
    [[nodiscard]] ui64 GetStartOffset() const;
    [[nodiscard]] ui64 GetSize() const;
};

class TWriteRequestHandler: public TIORequestsHandler
{
public:
    struct TPersistentBufferWriteMeta
    {
        ui8 Index;
        ui64 Lsn;

        TPersistentBufferWriteMeta(ui8 index, ui64 lsn)
            : Index(index)
            , Lsn(lsn)
        {}
    };

    TWriteRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui32 vChunkIndex,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TWriteRequestHandler() override = default;

    NWilson::TSpan& GetChildSpan(ui64 requestId, ui8 persistentBufferIndex);

    bool IsCompleted(ui64 requestId) override;

    void OnWriteRequested(ui64 requestId, ui8 persistentBufferIndex, ui64 lsn);

    [[nodiscard]] TVector<TPersistentBufferWriteMeta> GetWritesMeta() const;

    [[nodiscard]] NThreading::TFuture<TWriteBlocksLocalResponse>
    GetFuture() const;

    [[nodiscard]] TGuardedSgList GetData();

    void SetResponse(NProto::TError error);

private:
    std::shared_ptr<TWriteBlocksLocalRequest> Request;
    NThreading::TPromise<TWriteBlocksLocalResponse> Future;
    const ui8 RequiredAckCount = 3;
    ui8 AckCount = 0;
    ui8 AcksMask = 0;
    std::unordered_map<ui64, TPersistentBufferWriteMeta> WriteMetaByRequestId;
};

class TSyncRequestHandler: public TBaseRequestHandler
{
public:
    struct TSyncRequest
    {
        ui64 StartIndex;
        ui64 Lsn;
    };

    TSyncRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui32 vChunkIndex,
        ui8 persistentBufferIndex,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TSyncRequestHandler() override = default;

    NWilson::TSpan& GetChildSpan(ui64 requestId);

    [[nodiscard]] bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

    [[nodiscard]] ui64 OnSyncRequested(ui64 startIndex, ui64 lsn);

    [[nodiscard]] const TVector<TSyncRequest>& GetSyncRequests() const;

    [[nodiscard]] TVector<NKikimr::NDDisk::TBlockSelector> GetBlockSelectors() const;
    [[nodiscard]] TVector<ui64> GetLsns() const;

private:
    ui8 PersistentBufferIndex;
    TVector<TSyncRequest> SyncRequests;
};

////////////////////////////////////////////////////////////////////////////////

class TEraseRequestHandler: public TBaseRequestHandler
{
public:
    TEraseRequestHandler(
        NActors::TActorSystem* actorSystem,
        std::shared_ptr<TSyncRequestHandler> syncRequestHandler);

    ~TEraseRequestHandler() override = default;

    NWilson::TSpan& GetChildSpan(ui64 requestId);

    [[nodiscard]] bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

    [[nodiscard]] TVector<NKikimr::NDDisk::TBlockSelector> GetBlockSelectors() const;
    [[nodiscard]] TVector<ui64> GetLsns() const;

private:
    std::shared_ptr<TSyncRequestHandler> SyncRequestHandler;
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

    bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] NThreading::TFuture<TReadBlocksLocalResponse>
    GetFuture() const;

    [[nodiscard]] TGuardedSgList GetData();

    void SetResponse(NProto::TError error);

private:
    std::shared_ptr<TReadBlocksLocalRequest> Request;
    NThreading::TPromise<TReadBlocksLocalResponse> Future;
};

class TOverallAckRequestHandler: public TBaseRequestHandler
{
public:
    TOverallAckRequestHandler(
        NActors::TActorSystem* actorSystem,
        NWilson::TTraceId traceId,
        TString name,
        ui64 tabletId,
        ui8 requiredAckCount);

    ~TOverallAckRequestHandler() override = default;

    NWilson::TSpan GetChildSpan(ui64 requestId, TString eventName);

    [[nodiscard]] bool IsCompleted() const;
    bool IsCompleted(ui64 requestId) override;
    void RegisterCompetedRequest() {
        ++AckCount;
    }

    [[nodiscard]] ui8 GetRequiredAckCount() const {
        return RequiredAckCount;
    }

private:
    const ui8 RequiredAckCount;
    ui8 AckCount = 0;
    TString Name;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
