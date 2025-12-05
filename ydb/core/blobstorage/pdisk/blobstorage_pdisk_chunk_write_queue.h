#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

namespace NKikimr {
namespace NPDisk {

class TChunkWritePiece;

class TChunkWritePieceQueue {
public:
    TChunkWritePieceQueue() = default;
    bool Empty() const {
        return SizeCounter.load(std::memory_order_relaxed) == 0;
    }
    size_t Size() const {
        return SizeCounter.load(std::memory_order_relaxed);
    }
    TChunkWritePiece* Dequeue() {
        TChunkWritePiece* item;
        QueueImpl.Dequeue(&item);
        SizeCounter.fetch_sub(1, std::memory_order_relaxed);
        return item;
    }
    void Enqueue(TChunkWritePiece* item) {
        QueueImpl.Enqueue(item);
        SizeCounter.fetch_add(1, std::memory_order_relaxed);
    }
private:
    TLockFreeQueue<TChunkWritePiece*> QueueImpl;
    std::atomic<size_t> SizeCounter = 0;
};

} // NPDisk
} // NKikimr
