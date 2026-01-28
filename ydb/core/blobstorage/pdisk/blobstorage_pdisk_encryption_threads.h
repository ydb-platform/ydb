#include "blobstorage_pdisk_completion.h"
#include "blobstorage_pdisk_util_countedqueuemanyone.h"

#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

#include <queue>
#include <variant>

namespace NKikimr {
namespace NPDisk {

class TChunkWritePiece;

class TEncryptionThread : public ISimpleThread {
public:
    TEncryptionThread(TString name);
    void *ThreadProc() override;
    void Schedule(TChunkWritePiece *piece);
    size_t GetQueuedActions();
private:
    TCountedQueueManyOne<TChunkWritePiece, 4 << 10> Queue;
    TString Name;
};
class TEncryptionThreads {
public:
    static const size_t MAX_THREAD_COUNT = 4;
    TVector<THolder<TEncryptionThread>> Threads;
    size_t AvailableThreads;

    TEncryptionThreads(size_t threadsCount);
    void SetThreadCount(size_t threadsCount);

    // Schedule action execution
    // pass action = nullptr to quit
    void Schedule(TChunkWritePiece* piece) noexcept;

    void StopWork();
    void Join();
    void Stop();
};


} // NPDisk
} // NKikimr
