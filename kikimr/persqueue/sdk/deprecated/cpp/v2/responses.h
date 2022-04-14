#pragma once

#include "types.h"

#include <library/cpp/threading/future/future.h>
#include <util/generic/string.h>

namespace NPersQueue {

struct TProducerCreateResponse {
    TProducerCreateResponse(TWriteResponse&& response)
        : Response(std::move(response))
    {
    }

    TWriteResponse Response;
};

struct TProducerCommitResponse {
    TProducerCommitResponse(TProducerSeqNo seqNo, TData data, TWriteResponse&& response)
        : Response(std::move(response))
        , Data(std::move(data))
        , SeqNo(seqNo)
    {
    }

    TWriteResponse Response;
    TData Data;
    TProducerSeqNo SeqNo;
};


struct TConsumerCreateResponse {
    TConsumerCreateResponse(TReadResponse&& response)
        : Response(std::move(response))
    {
    }

    //will contain Error or Init
    TReadResponse Response;
};

enum EMessageType {
    EMT_LOCK,
    EMT_RELEASE,
    EMT_DATA,
    EMT_ERROR,
    EMT_STATUS,
    EMT_COMMIT
};

struct TLockInfo {
    ui64 ReadOffset = 0;
    ui64 CommitOffset = 0;
    bool VerifyReadOffset = false;

    TLockInfo() = default;

    // compatibility with msvc2015
    TLockInfo(ui64 readOffset, ui64 commitOffset, bool verifyReadOffset)
        : ReadOffset(readOffset)
        , CommitOffset(commitOffset)
        , VerifyReadOffset(verifyReadOffset)
    {}
};

static EMessageType GetType(const TReadResponse& response) {
    if (response.HasData()) return EMT_DATA;
    if (response.HasError()) return EMT_ERROR;
    if (response.HasCommit()) return EMT_COMMIT;
    if (response.HasPartitionStatus()) return EMT_STATUS;
    if (response.HasRelease()) return EMT_RELEASE;
    if (response.HasLock()) return EMT_LOCK;

    // for no warn return anything.
    return EMT_LOCK;
}



struct TConsumerMessage {
    EMessageType Type;
    //for DATA/ERROR/LOCK/RELEASE/COMMIT/STATUS:
    //will contain Error, Data, Lock or Release and be consistent with Type
    TReadResponse Response;

    //for LOCK only:
    mutable NThreading::TPromise<TLockInfo> ReadyToRead;

    TConsumerMessage(TReadResponse&& response, NThreading::TPromise<TLockInfo>&& readyToRead)
        : Type(EMT_LOCK)
        , Response(std::move(response))
        , ReadyToRead(std::move(readyToRead))
    {
        Y_VERIFY(GetType(Response) == EMT_LOCK);
    }


    explicit TConsumerMessage(TReadResponse&& response)
        : Type(GetType(response))
        , Response(std::move(response))
    {
    }
};

} // namespace NPersQueue
