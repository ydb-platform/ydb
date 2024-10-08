#include "hazard_ptr.h"

#include "private.h"

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ring_queue.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/threading/at_fork.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yt/containers/intrusive_linked_list.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/memory/free_list.h>

#include <library/cpp/yt/misc/tls.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = LockFreeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_THREAD_LOCAL(THazardPointerSet, HazardPointers);

//! A simple container based on free list which supports only Enqueue and DequeueAll.
template <class T>
class TRetireQueue
{
private:
    struct TNode
        : public TFreeListItemBase<TNode>
    {
        T Value;

        TNode() = default;

        explicit TNode(T&& value)
            : Value(std::move(value))
        { }
    };

    TFreeList<TNode> Impl_;

    void EraseList(TNode* node)
    {
        while (node) {
            auto* next = node->Next.load(std::memory_order::acquire);
            delete node;
            node = next;
        }
    }

public:
    ~TRetireQueue()
    {
        EraseList(Impl_.ExtractAll());
    }

    template <typename TCallback>
    void DequeueAll(TCallback&& callback)
    {
        auto* head = Impl_.ExtractAll();

        auto cleanup = Finally([this, head] {
            EraseList(head);
        });

        auto* ptr = head;
        while (ptr) {
            callback(ptr->Value);
            ptr = ptr->Next;
        }
    }

    void Enqueue(T&& value)
    {
        Impl_.Put(new TNode(std::forward<T>(value)));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRetiredPtr
{
    TPackedPtr PackedPtr;
    THazardPtrReclaimer Reclaimer;
};

struct THazardThreadState
{
    THazardPointerSet* const HazardPointers;

    TIntrusiveLinkedListNode<THazardThreadState> RegistryNode;
    TRingQueue<TRetiredPtr> RetireList;
    TCompactVector<void*, 64> ProtectedPointers;
    bool Reclaiming = false;

    explicit THazardThreadState(THazardPointerSet* hazardPointers)
        : HazardPointers(hazardPointers)
    { }
};

YT_DEFINE_THREAD_LOCAL(THazardThreadState*, HazardThreadState);
YT_DEFINE_THREAD_LOCAL(bool, HazardThreadStateDestroyed);

////////////////////////////////////////////////////////////////////////////////

class THazardPointerManager
{
public:
    struct THazardThreadStateToRegistryNode
    {
        auto operator() (THazardThreadState* state) const
        {
            return &state->RegistryNode;
        }
    };

    static THazardPointerManager* Get()
    {
        return LeakySingleton<THazardPointerManager>();
    }

    void InitThreadState();

    void RetireHazardPointer(TPackedPtr packedPtr, THazardPtrReclaimer reclaimer);

    void ReclaimHazardPointers(bool flush);

private:
    std::atomic<int> ThreadCount_ = 0;

    TRetireQueue<TRetiredPtr> RetireQueue_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ThreadRegistryLock_);
    TIntrusiveLinkedList<THazardThreadState, THazardThreadStateToRegistryNode> ThreadRegistry_;

    THazardPointerManager();

    void Shutdown();

    bool TryReclaimHazardPointers();
    bool DoReclaimHazardPointers(THazardThreadState* threadState);

    THazardThreadState* AllocateThreadState();
    void DestroyThreadState(THazardThreadState* ptr);

    void BeforeFork();
    void AfterForkParent();
    void AfterForkChild();

    DECLARE_LEAKY_SINGLETON_FRIEND()
};

////////////////////////////////////////////////////////////////////////////////

static void* HazardPointerManagerInitializer = [] {
    THazardPointerManager::Get();
    return nullptr;
}();

////////////////////////////////////////////////////////////////////////////////

THazardPointerManager::THazardPointerManager()
{
    NThreading::RegisterAtForkHandlers(
        [this] { BeforeFork(); },
        [this] { AfterForkParent(); },
        [this] { AfterForkChild(); });
}

void THazardPointerManager::Shutdown()
{
    if (auto* logFile = TryGetShutdownLogFile()) {
        ::fprintf(logFile, "%s\t*** Hazard Pointer Manager shutdown started (ThreadCount: %d)\n",
            GetInstant().ToString().c_str(),
            ThreadCount_.load());
    }

    int count = 0;
    RetireQueue_.DequeueAll([&] (TRetiredPtr& item) {
        item.Reclaimer(item.PackedPtr);
        ++count;
    });

    if (auto* logFile = TryGetShutdownLogFile()) {
        ::fprintf(logFile, "%s\t*** Hazard Pointer Manager shutdown completed (DeletedPtrCount: %d)\n",
            GetInstant().ToString().c_str(),
            count);
    }
}

void THazardPointerManager::RetireHazardPointer(TPackedPtr packedPtr, THazardPtrReclaimer reclaimer)
{
    auto* threadState = HazardThreadState();
    if (Y_UNLIKELY(!threadState)) {
        if (HazardThreadStateDestroyed()) {
            // Looks like a global shutdown.
            reclaimer(packedPtr);
            return;
        }
        InitThreadState();
        threadState = HazardThreadState();
    }

    threadState->RetireList.push({packedPtr, reclaimer});

    if (threadState->Reclaiming) {
        return;
    }

    int threadCount = ThreadCount_.load(std::memory_order::relaxed);
    while (std::ssize(threadState->RetireList) >= std::max(2 * threadCount, 1)) {
        DoReclaimHazardPointers(threadState);
    }
}

bool THazardPointerManager::TryReclaimHazardPointers()
{
    auto* threadState = HazardThreadState();
    if (!threadState || threadState->RetireList.empty()) {
        return false;
    }

    YT_VERIFY(!threadState->Reclaiming);

    bool hasNewPointers = DoReclaimHazardPointers(threadState);
    int threadCount = ThreadCount_.load(std::memory_order::relaxed);
    return
        hasNewPointers ||
        std::ssize(threadState->RetireList) > threadCount;
}

void THazardPointerManager::ReclaimHazardPointers(bool flush)
{
    if (flush) {
        while (TryReclaimHazardPointers());
    } else {
        TryReclaimHazardPointers();
    }
}

void THazardPointerManager::InitThreadState()
{
    if (!HazardThreadState()) {
        YT_VERIFY(!HazardThreadStateDestroyed());
        HazardThreadState() = AllocateThreadState();
    }
}

YT_PREVENT_TLS_CACHING THazardThreadState* THazardPointerManager::AllocateThreadState()
{
    auto* threadState = new THazardThreadState(&HazardPointers());

    struct THazardThreadStateDestroyer
    {
        THazardThreadState* ThreadState;

        ~THazardThreadStateDestroyer()
        {
            THazardPointerManager::Get()->DestroyThreadState(ThreadState);
        }
    };

    // Unregisters thread from hazard ptr manager on thread exit.
    thread_local THazardThreadStateDestroyer destroyer{threadState};

    {
        auto guard = WriterGuard(ThreadRegistryLock_);
        ThreadRegistry_.PushBack(threadState);
        ++ThreadCount_;
    }

    if (auto* logFile = TryGetShutdownLogFile()) {
        ::fprintf(logFile, "%s\t*** Hazard Pointer Manager thread state allocated (ThreadId: %" PRISZT ")\n",
            GetInstant().ToString().c_str(),
            GetCurrentThreadId());
    }

    return threadState;
}

bool THazardPointerManager::DoReclaimHazardPointers(THazardThreadState* threadState)
{
    threadState->Reclaiming = true;

    // Collect protected pointers.
    auto& protectedPointers = threadState->ProtectedPointers;
    YT_VERIFY(protectedPointers.empty());

    {
        auto guard = ForkFriendlyReaderGuard(ThreadRegistryLock_);
        for (
            auto* current = ThreadRegistry_.GetFront();
            current;
            current = current->RegistryNode.Next)
        {
            for (const auto& hazardPointer : *current->HazardPointers) {
                if (auto* ptr = hazardPointer.load()) {
                    protectedPointers.push_back(ptr);
                }
            }
        }
    }

    std::sort(protectedPointers.begin(), protectedPointers.end());

    auto& retireList = threadState->RetireList;

    // Append global RetireQueue_ to local retireList.
    RetireQueue_.DequeueAll([&] (auto item) {
        retireList.push(item);
    });

    YT_LOG_TRACE_IF(
        !protectedPointers.empty(),
        "Scanning hazard pointers (Candidates: %v, Protected: %v)",
        MakeFormattableView(TRingQueueIterableWrapper(retireList), [&] (auto* builder, auto item) {
            builder->AppendFormat("%v", TTaggedPtr<void>::Unpack(item.PackedPtr).Ptr);
        }),
        MakeFormattableView(protectedPointers, [&] (auto* builder, auto ptr) {
            builder->AppendFormat("%v", ptr);
        }));

    size_t pushedCount = 0;
    auto popCount = retireList.size();
    while (popCount-- > 0) {
        auto item = std::move(retireList.front());
        retireList.pop();

        void* ptr = TTaggedPtr<void>::Unpack(item.PackedPtr).Ptr;
        if (std::binary_search(protectedPointers.begin(), protectedPointers.end(), ptr)) {
            retireList.push(item);
            ++pushedCount;
        } else {
            item.Reclaimer(item.PackedPtr);
        }
    }

    protectedPointers.clear();

    threadState->Reclaiming = false;

    YT_VERIFY(pushedCount <= retireList.size());
    return pushedCount < retireList.size();
}

void THazardPointerManager::DestroyThreadState(THazardThreadState* threadState)
{
    {
        auto guard = WriterGuard(ThreadRegistryLock_);
        ThreadRegistry_.Remove(threadState);
        --ThreadCount_;
    }

    // Scan threadState->RetireList and move to blocked elements to global RetireQueue_.

    DoReclaimHazardPointers(threadState);

    int count = 0;
    while (!threadState->RetireList.empty()) {
        RetireQueue_.Enqueue(std::move(threadState->RetireList.front()));
        threadState->RetireList.pop();
        ++count;
    }

    if (auto* logFile = TryGetShutdownLogFile()) {
        ::fprintf(logFile, "%s\t*** Hazard Pointer Manager thread state destroyed (ThreadId: %" PRISZT ", RetiredPtrCount: %d)\n",
            GetInstant().ToString().c_str(),
            GetCurrentThreadId(),
            count);
    }

    delete threadState;

    HazardThreadState() = nullptr;
    HazardThreadStateDestroyed() = true;
}

void THazardPointerManager::BeforeFork()
{
    ThreadRegistryLock_.AcquireWriter();
}

void THazardPointerManager::AfterForkParent()
{
    ThreadRegistryLock_.ReleaseWriter();
}

void THazardPointerManager::AfterForkChild()
{
    ThreadRegistry_.Clear();
    ThreadCount_ = 0;

    if (HazardThreadState()) {
        ThreadRegistry_.PushBack(HazardThreadState());
        ThreadCount_ = 1;
    }

    ThreadRegistryLock_.ReleaseWriter();
}

////////////////////////////////////////////////////////////////////////////////

void InitHazardThreadState()
{
    THazardPointerManager::Get()->InitThreadState();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

void RetireHazardPointer(TPackedPtr packedPtr, THazardPtrReclaimer reclaimer)
{
    NYT::NDetail::THazardPointerManager::Get()->RetireHazardPointer(packedPtr, reclaimer);
}

void ReclaimHazardPointers(bool flush)
{
    NYT::NDetail::THazardPointerManager::Get()->ReclaimHazardPointers(flush);
}

////////////////////////////////////////////////////////////////////////////////

THazardPtrReclaimGuard::~THazardPtrReclaimGuard()
{
    ReclaimHazardPointers();
}

////////////////////////////////////////////////////////////////////////////////

THazardPtrReclaimOnContextSwitchGuard::THazardPtrReclaimOnContextSwitchGuard()
    : NConcurrency::TContextSwitchGuard(
        [] { ReclaimHazardPointers(); },
        nullptr)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
