#include "blobstorage_pdisk_encryption_threads.h"
#include "blobstorage_pdisk_requestimpl.h"

#include <ydb/core/util/hp_timer_helpers.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr {
namespace NPDisk {

TEncryptionThread::TEncryptionThread(TString name) : Name(name) {}

size_t TEncryptionThread::GetQueuedActions() {
    return Queue.GetWaitingSize();
}

void* TEncryptionThread::ThreadProc() {
    SetCurrentThreadName(Name.data());
    bool isWorking = true;

    while (isWorking) {
        TAtomicBase itemCount = Queue.GetWaitingSize();
        if (itemCount > 0) {
            for (TAtomicBase idx = 0; idx < itemCount; ++idx) {
                TChunkWritePiece *piece = Queue.Pop();
                if (piece == nullptr) {
                    isWorking = false;
                } else {
                    piece->Process();
                }
            }
        } else {
            Queue.ProducedWaitI();
        }
    }
    return nullptr;
}

void TEncryptionThread::Schedule(TChunkWritePiece *piece) {
    Queue.Push(piece);
}

TEncryptionThreads::TEncryptionThreads(size_t threadsCount) {
    Y_VERIFY(threadsCount <= MAX_THREAD_COUNT, "too many encryption threads");
    Threads.reserve(16);
    for (size_t i = 0; i < std::min(threadsCount, size_t(2)); i++) {
        Threads.push_back(MakeHolder<TEncryptionThread>(TStringBuilder() << "PdEncrypt" << i));
        Threads.back()->Start();
    }
    AvailableThreads = threadsCount;
}

void TEncryptionThreads::SetThreadCount(size_t threadsCount) {
    Y_VERIFY(threadsCount <= MAX_THREAD_COUNT, "too many encryption threads");
    for (size_t i = Threads.size(); i < threadsCount; i++) {
        Threads.push_back(MakeHolder<TEncryptionThread>(TStringBuilder() << "PdEncrypt" << i));
        Threads.back()->Start();
    }
    AvailableThreads = threadsCount;
}


void TEncryptionThreads::StopWork() {
    for (auto& thread : Threads) {
        thread->Schedule(nullptr);
    }
}

void TEncryptionThreads::Join() {
    for (auto& thread : Threads) {
        thread->Join();
    }
    Threads.clear();
}

void TEncryptionThreads::Stop() {
    StopWork();
    Join();
}

void TEncryptionThreads::Schedule(TChunkWritePiece* piece) noexcept {
    if (!piece) {
        StopWork();
        return;
    }

    if (!AvailableThreads) {
        piece->Process();
        return;
    }

    auto min_it = Threads.begin();
    auto minQueueSize = (*min_it)->GetQueuedActions();
    for (auto it = Threads.begin() + 1; it != Threads.begin() + AvailableThreads; ++it) {
        auto queueSize = (*it)->GetQueuedActions();
        if (queueSize < minQueueSize) {
            minQueueSize = queueSize;
            min_it = it;
        }
    }
    Y_VERIFY(min_it != Threads.end());
    (*min_it)->Schedule(piece);
}

} // NPDisk
} // NKikimr
