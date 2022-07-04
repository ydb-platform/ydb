#include <util/generic/algorithm.h>
#include <util/generic/noncopyable.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/info.h>
#include <util/system/spinlock.h>
#include <util/system/thread.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

#define PLATFORM_CACHE_LINE 64

template <typename T>
struct TNode: private TNonCopyable {
    TNode* Next;
    T Item;

    TNode(const T& item)
        : Next(nullptr)
        , Item(item)
    {
    }

    TNode(T&& item)
        : Next(nullptr)
        , Item(std::move(item))
    {
    }

    char Padding[4000];
};

template <typename TNode>
inline void DeleteList(TNode* node) {
    while (node != nullptr) {
        TNode* next = node->Next;
        delete node;
        node = next;
    }
}

typedef void* TMessageLink;

////////////////////////////////////////////////////////////////////////////////
// http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue
// Very fast (as there is no CAS operation), but not exactly lock-free:
// one blocked producer could prevent consumer from obtaining new nodes.

template <typename T = TMessageLink>
class TFastLF_M1_Queue: private TNonCopyable {
private:
    union {
        TNode<T>* Head;
        char Pad1[PLATFORM_CACHE_LINE];
    };
    union {
        TNode<T>* Tail;
        char Pad2[PLATFORM_CACHE_LINE];
    };

public:
    using TItem = T;

    TFastLF_M1_Queue() {
        Head = Tail = new TNode<T>(T());
    }

    ~TFastLF_M1_Queue() {
        DeleteList(Head);
    }

    template <typename TT>
    void Push(TT&& item) {
        Enqueue(new TNode<T>(std::forward<TT>(item)));
    }

    void Enqueue(TNode<T>* node) {
        // our goal is to avoid expensive CAS here,
        // but now consumer will be blocked until new tail linked.
        // fortunately 'window of inconsistency' is extremely small.
        TNode<T>* prev = AtomicSwap(&Tail, node);
        AtomicSet(prev->Next, node);
    }

    T Pop() {
        TNode<T>* next = AtomicGet(Head->Next);
        if (next) {
            auto item = std::move(next->Item);
            std::swap(Head, next); // no need atomic here
            delete next;
            return item;
        }
        return nullptr;
    }

    bool IsEmpty() const {
        TNode<T>* next = AtomicGet(Head->Next);
        return (next == nullptr);
    }
};

const size_t NUMBER_OF_PUSHERS = NSystemInfo::NumberOfCpus() + 4;
const size_t NUMBER_OF_QUEUES = NSystemInfo::NumberOfCpus() / 4;

template <typename TQueueType>
class TQueueTestProcs: public TTestBase {
private:
    UNIT_TEST_SUITE_DEMANGLE(TQueueTestProcs<TQueueType>);
    UNIT_TEST(RndPush1M_Queues)
    UNIT_TEST_SUITE_END();

public:
    void RndPush1M_Queues() {
        TQueueType* queue = new TQueueType[NUMBER_OF_QUEUES];

        class TPusherThread: public ISimpleThread {
        public:
            TPusherThread(TQueueType* queues, char* start)
                : Queues(queues)
                , Arg(start)
            {
            }

            TQueueType* Queues;
            char* Arg;

            void* ThreadProc() override {
                auto counters = new int[NUMBER_OF_QUEUES];
                for (size_t i = 0; i < NUMBER_OF_QUEUES; ++i)
                    counters[i] = 0;
#if defined(_msan_enabled_) || defined(_asan_enabled_)
                int limit = 100000;
#else
                int limit = 1000000;
#endif
                for (int i = 0; i < limit; ++i) {
                    size_t rnd = GetCycleCount() % NUMBER_OF_QUEUES;
                    int cookie = counters[rnd]++;
                    Queues[rnd].Push(Arg + cookie);
                }

                for (size_t i = 0; i < NUMBER_OF_QUEUES; ++i) {
                    Queues[i].Push((void*)1ULL);
                }

                delete[] counters;
                return nullptr;
            }
        };

        class TPopperThread: public ISimpleThread {
        public:
            TPopperThread(TQueueType* queue, char* base)
                : Queue(queue)
                , Base(base)
            {
            }

            TQueueType* Queue;
            char* Base;

            void* ThreadProc() override {
                auto counters = new int[NUMBER_OF_PUSHERS];
                for (size_t i = 0; i < NUMBER_OF_PUSHERS; ++i)
                    counters[i] = 0;

                for (size_t fin = 0; fin < NUMBER_OF_PUSHERS;) {
                    auto msg = Queue->Pop();
                    if (msg == nullptr)
                        continue;
                    if (msg == (void*)1ULL) {
                        ++fin;
                        continue;
                    }
                    auto shift = (char*)msg - Base;
                    auto pusherNum = shift / 20000000;
                    auto msgNum = shift % 20000000;

                    if (counters[pusherNum] != msgNum) {
                        Cerr << counters[pusherNum] << " " << msgNum << Endl;
                    }

                    UNIT_ASSERT_EQUAL(counters[pusherNum], msgNum);
                    ++counters[pusherNum];
                }

                delete[] counters;
                return nullptr;
            }
        };

        TVector<TAutoPtr<TPopperThread>> poppers;
        TVector<TAutoPtr<TPusherThread>> pushers;

        for (size_t i = 0; i < NUMBER_OF_QUEUES; ++i) {
            poppers.emplace_back(new TPopperThread(&queue[i], (char*)queue));
            poppers.back()->Start();
        }

        for (size_t i = 0; i < NUMBER_OF_PUSHERS; ++i) {
            pushers.emplace_back(
                new TPusherThread(queue, (char*)queue + 20000000 * i));
            pushers.back()->Start();
        }

        for (size_t i = 0; i < NUMBER_OF_QUEUES; ++i) {
            poppers[i]->Join();
        }

        for (size_t i = 0; i < NUMBER_OF_PUSHERS; ++i) {
            pushers[i]->Join();
        }

        delete[] queue;
    }
};

UNIT_TEST_SUITE_REGISTRATION(TQueueTestProcs<TFastLF_M1_Queue<>>);
