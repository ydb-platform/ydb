#include <util/thread/lfqueue.h>

#include <queue>
#include <variant>

namespace NKikimr {
namespace NPDisk {

class TChunkWritePiece;

class TChunkWritePieceQueue {
public:
    TChunkWritePieceQueue() = delete;
    TChunkWritePieceQueue(int threadsCount);

    bool IsEmpty();
    size_t Size();
    TChunkWritePiece* Dequeue();
    void Enqueue(TChunkWritePiece* item);
private:
    bool SingleThread;
    std::queue<TChunkWritePiece*>* GetQueue();
    TLockFreeQueue<TChunkWritePiece*>* GetLfQueue();
    std::variant<std::queue<TChunkWritePiece*>, THolder<TLockFreeQueue<TChunkWritePiece*>>> QueueImpl;
    std::atomic<size_t> SizeCounter = 0;
};

} // NPDisk
} // NKikimr
