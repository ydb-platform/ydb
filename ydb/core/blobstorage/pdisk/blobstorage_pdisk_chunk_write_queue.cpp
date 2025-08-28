#include "blobstorage_pdisk_chunk_write_queue.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr {
namespace NPDisk {

TChunkWritePieceQueue::TChunkWritePieceQueue(int threadsCount) : SingleThread(threadsCount == 1) {
    Y_VERIFY_S(0 < threadsCount && threadsCount < 256, "invalid EncrypthionThreadsCount");
    if (SingleThread) {
        QueueImpl = std::queue<TChunkWritePiece*>();
    } else {
        QueueImpl = MakeHolder<TLockFreeQueue<TChunkWritePiece*>>();
    }
}

std::queue<TChunkWritePiece*>* TChunkWritePieceQueue::GetQueue() {
    return &std::get<0>(QueueImpl);
}

TLockFreeQueue<TChunkWritePiece*>* TChunkWritePieceQueue::GetLfQueue() {
    auto& ptr = std::get<1>(QueueImpl);
    return ptr.Get();
}

bool TChunkWritePieceQueue::IsEmpty() {
    if (SingleThread) {
        return GetQueue()->empty();
    } else {
        return SizeCounter.load(std::memory_order_relaxed) == 0;
    }
}

size_t TChunkWritePieceQueue::Size() {
    if (SingleThread) {
        return GetQueue()->size();
    } else {
        return SizeCounter.load(std::memory_order_relaxed);
    }
}

TChunkWritePiece* TChunkWritePieceQueue::Dequeue() {
    TChunkWritePiece* item;
    if (SingleThread) {
        item = GetQueue()->front();
        GetQueue()->pop();
    } else {
        GetLfQueue()->Dequeue(&item);
        SizeCounter.fetch_sub(1, std::memory_order_release);
    }
    return item;
}

void TChunkWritePieceQueue::Enqueue(TChunkWritePiece* item) {
    if (SingleThread) {
        GetQueue()->push(item);
    } else {
        GetLfQueue()->Enqueue(item);
        SizeCounter.fetch_add(1, std::memory_order_release);
    }
}

} // NPDisk
} // NKikimr
