#pragma once

#include <ydb/library/actors/core/actor.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockSize = 4096;

////////////////////////////////////////////////////////////////////////////////

class IRequest {
public:
    TActorId Sender;
    // In bytes.
    ui64 StartIndex;

    IRequest(
        TActorId sender,
        ui64 startIndex);

    virtual ~IRequest() = default;

    [[nodiscard]] virtual ui64 GetDataSize() const = 0;

    [[nodiscard]] virtual bool IsCompleted(ui64 requestId) = 0;
};

class TWriteRequest : public IRequest {
public:
    TWriteRequest(
        TActorId sender,
        ui64 startIndex,
        TString data);

    ~TWriteRequest() override = default;

    [[nodiscard]] const TString& GetData() const;

    ui64 GetDataSize() const override;

    void OnDDiskWriteRequested(ui64 requestId, ui8 ddiskIndex);
    
    bool IsCompleted(ui64 requestId) override;

private:
    const TString Data;
    const ui8 RequiredAckCount = 3;
    ui8 AckCount = 0;
    ui8 DDisksAcksMask = 0;
    TMap<ui64, ui8> DDiskIndexByRequestId;
};

class TReadRequest : public IRequest {
public:
    ui64 BlocksCount;
    
    TReadRequest(
        TActorId sender,
        ui64 startIndex,
        ui64 blocksCount);

    ~TReadRequest() override = default;
    
    ui64 GetDataSize() const override;
    
    bool IsCompleted(ui64 requestId) override;
};

}   // namespace NYdb::NBS::NStorage::NPartitionDirect
