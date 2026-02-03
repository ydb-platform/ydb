#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 4096;

////////////////////////////////////////////////////////////////////////////////

class IRequest {
public:
    ui64 StartIndex;
    NWilson::TSpan Span;
    std::unordered_map<ui64, NWilson::TSpan> ChildSpanByRequestId;

    IRequest(ui64 startIndex, NWilson::TSpan span);

    virtual ~IRequest() = default;

    [[nodiscard]] virtual ui64 GetDataSize() const = 0;

    [[nodiscard]] virtual bool IsCompleted(ui64 requestId) = 0;

    [[nodiscard]] ui64 GetStartOffset() const;

    void ChildSpanEndOk(ui64 childRequestId);

    void ChildSpanEndError(ui64 childRequestId, const TString& errorMessage);
};

class TWriteRequest : public IRequest {
public:
    struct TPersistentBufferWriteMeta {
        ui8 Index;
        ui64 Lsn;

        TPersistentBufferWriteMeta(ui8 index, ui64 lsn)
            : Index(index)
            , Lsn(lsn)
        {}
    };

    TWriteRequest(
        TActorId sender,
        ui64 cookie,
        ui64 startIndex,
        TString data,
        NWilson::TSpan span);

    ~TWriteRequest() override = default;

    [[nodiscard]] TActorId GetSender() const;

    [[nodiscard]] ui64 GetCookie() const;

    [[nodiscard]] const TString& GetData() const;

    ui64 GetDataSize() const override;

    void OnWriteRequested(
        ui64 requestId, ui8 persistentBufferIndex, ui64 lsn, NWilson::TSpan span);

    bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] TVector<TPersistentBufferWriteMeta> GetWritesMeta() const;

private:
    TActorId Sender;
    ui64 Cookie;
    const TString Data;
    const ui8 RequiredAckCount = 3;
    ui8 AckCount = 0;
    ui8 AcksMask = 0;
    std::unordered_map<ui64, TPersistentBufferWriteMeta> WriteMetaByRequestId;
};

class TFlushRequest : public IRequest {
public:
    TFlushRequest(
        ui64 startIndex,
        bool isErase,
        ui8 persistentBufferIndex,
        ui64 lsn,
        NWilson::TSpan span);

    ~TFlushRequest() override = default;

    ui64 GetDataSize() const override;

    bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] bool GetIsErase() const;

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

    [[nodiscard]] ui64 GetLsn() const;

    void OnFlushRequested(ui64 requestId, NWilson::TSpan span);

private:
    bool IsErase;
    ui8 PersistentBufferIndex;
    ui64 Lsn;
};

class TReadRequest : public IRequest {
public:
    TReadRequest(
        TActorId sender,
        ui64 cookie,
        ui64 startIndex,
        ui64 blocksCount,
        NWilson::TSpan span);

    ~TReadRequest() override = default;

    [[nodiscard]] TActorId GetSender() const;

    [[nodiscard]] ui64 GetCookie() const;

    ui64 GetDataSize() const override;

    bool IsCompleted(ui64 requestId) override;

    void OnReadRequested(ui64 requestId, NWilson::TSpan span);

private:
    TActorId Sender;
    ui64 Cookie;
    ui64 BlocksCount;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
