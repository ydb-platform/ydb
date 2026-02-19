#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TBaseRequestHandler
{
private:
    NActors::TActorSystem* const ActorSystem = nullptr;
    const TBlockRange64 Range;

public:
    TBaseRequestHandler(
        NActors::TActorSystem* actorSystem,
        TBlockRange64 range);

    virtual ~TBaseRequestHandler() = default;

    [[nodiscard]] NActors::TActorSystem* GetActorSystem() const;

    [[nodiscard]] ui64 GetStartIndex() const;
    [[nodiscard]] ui64 GetStartOffset() const;
    [[nodiscard]] ui64 GetSize() const;

    virtual bool IsCompleted(ui64 requestId) = 0;

    void ChildSpanEndOk(ui64 childRequestId);

    void ChildSpanEndError(ui64 childRequestId, const TString& errorMessage);

    NWilson::TSpan Span;
    std::unordered_map<ui64, NWilson::TSpan> ChildSpanByRequestId;
};

class TWriteRequestHandler: public TBaseRequestHandler
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
    TSyncRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui64 startIndex,
        ui8 persistentBufferIndex,
        ui64 lsn,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TSyncRequestHandler() override = default;

    [[nodiscard]] bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] ui64 GetLsn() const;

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

private:
    ui8 PersistentBufferIndex;
    ui64 Lsn;
};

class TEraseRequestHandler: public TBaseRequestHandler
{
public:
    TEraseRequestHandler(
        NActors::TActorSystem* actorSystem,
        ui64 startIndex,
        ui8 persistentBufferIndex,
        ui64 lsn,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TEraseRequestHandler() override = default;

    [[nodiscard]] bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] ui64 GetLsn() const;

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

private:
    ui8 PersistentBufferIndex;
    ui64 Lsn;
};

class TReadRequestHandler: public TBaseRequestHandler
{
public:
    TReadRequestHandler(
        NActors::TActorSystem* actorSystem,
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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
