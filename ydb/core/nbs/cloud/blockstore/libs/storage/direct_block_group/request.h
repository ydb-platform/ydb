#pragma once

#include <ydb/library/actors/util/rope.h>

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

    explicit TWriteRequestHandler(std::shared_ptr<TWriteBlocksLocalRequest> request);

    ~TWriteRequestHandler() override = default;

    [[nodiscard]] ui64 GetStartIndex() const override;

    [[nodiscard]] ui64 GetStartOffset() const override;

    [[nodiscard]] ui64 GetSize() const override;

    bool IsCompleted(ui64 requestId) override;

    void OnWriteRequested(
        ui64 requestId, ui8 persistentBufferIndex, ui64 lsn);

    [[nodiscard]] TVector<TPersistentBufferWriteMeta> GetWritesMeta() const;

    [[nodiscard]] NThreading::TFuture<TWriteBlocksLocalResponse> GetFuture() const;

    [[nodiscard]] TGuardedSgList GetData();

private:
    std::shared_ptr<TWriteBlocksLocalRequest> Request;
    NThreading::TPromise<TWriteBlocksLocalResponse> Future;
    const ui8 RequiredAckCount = 3;
    ui8 AckCount = 0;
    ui8 AcksMask = 0;
    std::unordered_map<ui64, TPersistentBufferWriteMeta> WriteMetaByRequestId;
};

class TFlushRequestHandler : public IRequestHandler {
public:
    TFlushRequestHandler(ui64 startIndex, ui8 persistentBufferIndex, ui64 lsn);

    ~TFlushRequestHandler() override = default;

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
    TEraseRequestHandler(ui64 startIndex, ui8 persistentBufferIndex, ui64 lsn);

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
    explicit TReadRequestHandler(std::shared_ptr<TReadBlocksLocalRequest> request);

    ~TReadRequestHandler() override = default;

    [[nodiscard]] ui64 GetStartIndex() const override;

    [[nodiscard]] ui64 GetStartOffset() const override;

    [[nodiscard]] ui64 GetSize() const override;

    bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] NThreading::TFuture<TReadBlocksLocalResponse> GetFuture() const;

    [[nodiscard]] TGuardedSgList GetData();


private:
    std::shared_ptr<TReadBlocksLocalRequest> Request;
    NThreading::TPromise<TReadBlocksLocalResponse> Future;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
