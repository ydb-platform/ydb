#pragma once

#include <ydb/library/actors/util/rope.h>
#include <ydb/library/actors/wilson/wilson_span.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 4096;

////////////////////////////////////////////////////////////////////////////////

class IRequestHandler {
public:
    IRequestHandler() = default;

    virtual ~IRequestHandler() = default;

    [[nodiscard]] virtual ui64 GetStartIndex() const = 0;

    [[nodiscard]] virtual ui64 GetStartOffset() const = 0;

    [[nodiscard]] virtual ui64 GetSize() const = 0;

    virtual bool IsCompleted(ui64 requestId) = 0;

    void ChildSpanEndOk(ui64 childRequestId);

    void ChildSpanEndError(ui64 childRequestId, const TString& errorMessage);

    NWilson::TSpan Span;
    std::unordered_map<ui64, NWilson::TSpan> ChildSpanByRequestId;
};

class TWriteRequestHandler : public IRequestHandler {
public:
    struct TPersistentBufferWriteMeta {
        ui8 Index;
        ui64 Lsn;

        TPersistentBufferWriteMeta(ui8 index, ui64 lsn)
            : Index(index)
            , Lsn(lsn)
        {}
    };

    TWriteRequestHandler(
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TWriteRequestHandler() override = default;

    NWilson::TTraceId GetChildSpan(ui64 requestId, ui8 persistentBufferIndex);

    [[nodiscard]] ui64 GetStartIndex() const override;

    [[nodiscard]] ui64 GetStartOffset() const override;

    [[nodiscard]] ui64 GetSize() const override;

    bool IsCompleted(ui64 requestId) override;

    void OnWriteRequested(
        ui64 requestId, ui8 persistentBufferIndex, ui64 lsn);

    [[nodiscard]] TVector<TPersistentBufferWriteMeta> GetWritesMeta() const;

    [[nodiscard]] NThreading::TFuture<TWriteBlocksLocalResponse> GetFuture() const;

    [[nodiscard]] TGuardedSgList GetData();

    void SetResponse();

private:
    std::shared_ptr<TWriteBlocksLocalRequest> Request;
    NThreading::TPromise<TWriteBlocksLocalResponse> Future;
    const ui8 RequiredAckCount = 3;
    ui8 AckCount = 0;
    ui8 AcksMask = 0;
    std::unordered_map<ui64, TPersistentBufferWriteMeta> WriteMetaByRequestId;
};

class TSyncRequestHandler : public IRequestHandler {
public:
    TSyncRequestHandler(
        ui64 startIndex,
        ui8 persistentBufferIndex,
        ui64 lsn,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TSyncRequestHandler() override = default;

    [[nodiscard]] ui64 GetStartIndex() const override;

    [[nodiscard]] ui64 GetStartOffset() const override;

    [[nodiscard]] ui64 GetSize() const override;

    [[nodiscard]] bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] ui64 GetLsn() const;

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

private:
    ui64 StartIndex;
    ui8 PersistentBufferIndex;
    ui64 Lsn;
};

class TEraseRequestHandler : public IRequestHandler {
public:
    TEraseRequestHandler(
        ui64 startIndex,
        ui8 persistentBufferIndex,
        ui64 lsn,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TEraseRequestHandler() override = default;

    [[nodiscard]] ui64 GetStartIndex() const override;

    [[nodiscard]] ui64 GetStartOffset() const override;

    [[nodiscard]] ui64 GetSize() const override;

    [[nodiscard]] bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] ui64 GetLsn() const;

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

private:
    ui64 StartIndex;
    ui8 PersistentBufferIndex;
    ui64 Lsn;
};


class TReadRequestHandler : public IRequestHandler {
public:
    TReadRequestHandler(
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId,
        ui64 tabletId);

    ~TReadRequestHandler() override = default;

    NWilson::TTraceId GetChildSpan(ui64 requestId, bool isReadPersistentBuffer);

    [[nodiscard]] ui64 GetStartIndex() const override;

    [[nodiscard]] ui64 GetStartOffset() const override;

    [[nodiscard]] ui64 GetSize() const override;

    bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] NThreading::TFuture<TReadBlocksLocalResponse> GetFuture() const;

    [[nodiscard]] TGuardedSgList GetData();

    void SetResponse();

private:
    std::shared_ptr<TReadBlocksLocalRequest> Request;
    NThreading::TPromise<TReadBlocksLocalResponse> Future;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
