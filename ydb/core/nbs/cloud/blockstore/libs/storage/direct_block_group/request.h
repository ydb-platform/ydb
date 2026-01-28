#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 4096;

////////////////////////////////////////////////////////////////////////////////

class IRequest {
public:
    ui64 StartIndex;

    IRequest(ui64 startIndex);

    virtual ~IRequest() = default;

    [[nodiscard]] virtual ui64 GetDataSize() const = 0;

    [[nodiscard]] virtual bool IsCompleted(ui64 requestId) = 0;

    [[nodiscard]] ui64 GetStartOffset() const;
};

class TWriteRequest : public IRequest {
public:;
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
        ui64 startIndex,
        TString data);

    ~TWriteRequest() override = default;

    [[nodiscard]] TActorId GetSender() const;

    [[nodiscard]] const TString& GetData() const;

    ui64 GetDataSize() const override;

    void OnWriteRequested(ui64 requestId, ui8 persistentBufferIndex, ui64 lsn);
    
    bool IsCompleted(ui64 requestId) override;

    [[nodiscard]] TVector<TPersistentBufferWriteMeta> GetWritesMeta() const;

private:
    TActorId Sender;
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
        ui64 lsn);

    ~TFlushRequest() override = default;

    ui64 GetDataSize() const override;

    bool IsCompleted(ui64 requestId) override;
    
    [[nodiscard]] bool GetIsErase() const;

    [[nodiscard]] ui8 GetPersistentBufferIndex() const;

    [[nodiscard]] ui64 GetLsn() const;

private:
    bool IsErase;
    ui8 PersistentBufferIndex;
    ui64 Lsn;
};

class TReadRequest : public IRequest {
public:    
    TReadRequest(
        TActorId sender,
        ui64 startIndex,
        ui64 blocksCount);

    ~TReadRequest() override = default;
    
    [[nodiscard]] TActorId GetSender() const;

    ui64 GetDataSize() const override;
    
    bool IsCompleted(ui64 requestId) override;

private:
    TActorId Sender;
    ui64 BlocksCount;
};

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
