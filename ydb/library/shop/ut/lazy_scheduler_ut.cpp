#include <ydb/library/shop/lazy_scheduler.h>

#include <util/generic/deque.h>
#include <util/random/random.h>
#include <util/string/vector.h>
#include <util/system/type_name.h>
#include <library/cpp/testing/unittest/registar.h>

#define SHOP_LAZY_SCHEDULER_UT_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES) \
    PROBE(LazyUtScheduleState, GROUPS(), \
      TYPES(TString), \
      NAMES("state")) \
    PROBE(LazyUtScheduleTask, GROUPS(), \
      TYPES(TString, ui64), \
      NAMES("queue", "cost")) \
    /**/

LWTRACE_DECLARE_PROVIDER(SHOP_LAZY_SCHEDULER_UT_PROVIDER)
LWTRACE_DEFINE_PROVIDER(SHOP_LAZY_SCHEDULER_UT_PROVIDER)

Y_UNIT_TEST_SUITE(ShopLazyScheduler) {
    LWTRACE_USING(SHOP_LAZY_SCHEDULER_UT_PROVIDER);
    using namespace NShop;

    template <class TRes>
    struct TTest {
        // We need Priority in tests to break ties in deterministic fashion
        struct TMyRes {
            using TCost = typename TRes::TCost;
            using TTag = typename TRes::TTag;

            struct TKey {
                typename TRes::TKey Key;
                int Priority;

                bool operator<(const TKey& rhs) const
                {
                   if (Key < rhs.Key) {
                       return true;
                   } else if (Key > rhs.Key) {
                       return false;
                   } else {
                       return Priority < rhs.Priority;
                   }
                }
            };

            inline static TKey OffsetKey(TKey key, typename TRes::TTag offset);
            template <class ConsumerT>
            inline static void SetKey(TKey& key, ConsumerT* consumer);
            inline static TTag GetTag(const TKey& key);
            inline static TKey MaxKey();
            template <class ConsumerT>
            inline static TKey ZeroKey(ConsumerT* consumer);
            inline static void ActivateKey(TKey& key, TTag vtime);
        };

        class TMyQueue;
        using TMySchedulable = TSchedulable<typename TMyRes::TCost>;
        using TMyConsumer = NLazy::TConsumer<TMyRes>;
        using TCtx = NLazy::TCtx;

        class TMyTask : public TMySchedulable {
        public:
            TMyQueue* Queue = nullptr;
        public:
            explicit TMyTask(typename TMyRes::TCost cost)
            {
                this->Cost = cost;
            }
        };

        class TMyQueue: public TMyConsumer {
        public:
            TDeque<TMyTask*> Tasks;
            int Priority;
            TMachineIdx MachineIdx = 0;
        public: // Interface for clients
            TMyQueue(TWeight w = 1, int priority = 0)
                : Priority(priority)
            {
                this->SetWeight(w);
            }

            ~TMyQueue()
            {
                for (auto i = Tasks.begin(), e = Tasks.end(); i != e; ++i) {
                    delete *i;
                }
            }

            void SetMachineIdx(TMachineIdx midx)
            {
                MachineIdx = midx;
            }

            void PushTask(TMyTask* task)
            {
                if (Empty()) { // Scheduler must be notified on first task in queue
                    this->Activate(MachineIdx);
                }
                task->Queue = this;
                Tasks.push_back(task);
            }

            void PushTaskFront(TMyTask* task)
            {
                if (Empty()) { // Scheduler must be notified on first task in queue
                    this->Activate(MachineIdx);
                }
                task->Queue = this;
                Tasks.push_front(task);
            }

            using TMyConsumer::Activate;
            using TMyConsumer::Deactivate;

            void Activate()
            {
                this->Activate(MachineIdx);
            }

            void Deactivate()
            {
                this->Deactivate(MachineIdx);
            }

            bool Empty() const
            {
                return Tasks.empty();
            }

        public: // Interface for scheduler
            TMySchedulable* PopSchedulable(const TCtx&) override
            {
                if (Tasks.empty()) {
                    return nullptr; // denial
                }
                UNIT_ASSERT(!Tasks.empty());
                TMyTask* task = Tasks.front();
                Tasks.pop_front();
                if (Tasks.empty()) {
                    this->Deactivate(MachineIdx);
                }
                return task;
            }

            void DeactivateAll() override
            {
                Deactivate();
            }
        };


        ///////////////////////////////////////////////////////////////////////////////

        class TMyScheduler;

        struct TMyShopState : public TShopState {
            explicit TMyShopState(size_t size = 1)
                : TShopState(size)
            {}

            void Freeze(TMachineIdx idx);
            void Unfreeze(TMachineIdx idx);
            void AddScheduler(TMyScheduler* scheduler);
        };

        ///////////////////////////////////////////////////////////////////////////////

        using TQueuePtr = std::shared_ptr<TMyQueue>;

        class TMyScheduler: public NLazy::TScheduler<TMyRes> {
        public:
            THashMap<TString, TQueuePtr> Queues;
            int Priority;
        public:
            explicit TMyScheduler(TMyShopState& ss, TWeight w = 1, int priority = 0)
                : Priority(priority)
            {
                ss.AddScheduler(this);
                this->SetWeight(w);
            }

            ~TMyScheduler()
            {
                this->UpdateCounters();
            }

            void AddQueue(const TString& name, TMachineIdx midx, const TQueuePtr& queue)
            {
                queue->SetName(name);
                queue->SetMachineIdx(midx);
                Queues.emplace(name, queue);
                this->Attach(queue.get());
            }

            void AddSubScheduler(const TString& name, TMyScheduler* scheduler)
            {
                scheduler->SetName(name);
                this->Attach(scheduler);
            }

            void DeleteQueue(const TString& name)
            {
                auto iter = Queues.find(name);
                if (iter != Queues.end()) {
                    TMyConsumer* consumer = iter->second.get();
                    consumer->Detach();
                    Queues.erase(iter);
                }
            }

            void DeleteSubScheduler(TMyScheduler* scheduler)
            {
                scheduler->Detach();
            }
        };

        ///////////////////////////////////////////////////////////////////////////////

        template <class ConsumerT>
        static void SetPriority(int& priority, ConsumerT const* consumer)
        {
            if (auto queue = dynamic_cast<TMyQueue const*>(consumer)) {
                priority = queue->Priority;
            } else if (auto sched = dynamic_cast<TMyScheduler const*>(consumer)) {
                priority = sched->Priority;
            } else {
                UNIT_FAIL("unable get priority of object of type: " << TypeName(consumer));
            }
        }

        static void Generate(TMyQueue* queue, const TString& tasks)
        {
            TVector<TString> v = SplitString(tasks, " ");
            for (size_t i = 0; i < v.size(); i++) {
                queue->PushTask(new TMyTask(FromString<typename TMyRes::TCost>(v[i])));
            }
        }

        static void GenerateFront(TMyQueue* queue, const TString& tasks)
        {
            TVector<TString> v = SplitString(tasks, " ");
            for (size_t i = 0; i < v.size(); i++) {
                queue->PushTaskFront(new TMyTask(FromString<typename TMyRes::TCost>(v[i])));
            }
        }

        static TString Schedule(TMyScheduler& sched, size_t count = size_t(-1), bool printcost = false)
        {
            TStringStream ss;
            while (count--) {
                LWPROBE(LazyUtScheduleState, sched.DebugString());
                TMyTask* task = static_cast<TMyTask*>(sched.PopSchedulable());
                if (!task) {
                    break;
                }
                LWPROBE(LazyUtScheduleTask, task->Queue->GetName(), task->Cost);
                ss << task->Queue->GetName();
                if (printcost) {
                    ss << task->Cost;
                }
                delete task;
            }
            return ss.Str();
        }
    };

    ///////////////////////////////////////////////////////////////////////////////

    template <class TRes>
    void TTest<TRes>::TMyShopState::Freeze(TMachineIdx idx)
    {
        TShopState::Freeze(idx);
    }

    template <class TRes>
    void TTest<TRes>::TMyShopState::Unfreeze(TMachineIdx idx)
    {
        TShopState::Unfreeze(idx);
    }

    template <class TRes>
    void TTest<TRes>::TMyShopState::AddScheduler(TMyScheduler* scheduler)
    {
        scheduler->SetShopState(this);
    }

    ///////////////////////////////////////////////////////////////////////////////

    template <class TRes>
    template <class ConsumerT>
    inline void TTest<TRes>::TMyRes::SetKey(
        typename TTest<TRes>::TMyRes::TKey& key,
        ConsumerT* consumer)
    {
        TRes::SetKey(key.Key, consumer);
        TTest<TRes>::SetPriority(key.Priority, consumer);
    }

    template <class TRes>
    inline typename TRes::TTag TTest<TRes>::TMyRes::GetTag(
        const typename TTest<TRes>::TMyRes::TKey& key)
    {
        return TRes::GetTag(key.Key);
    }

    template <class TRes>
    inline typename TTest<TRes>::TMyRes::TKey TTest<TRes>::TMyRes::OffsetKey(
        typename TTest<TRes>::TMyRes::TKey key,
        typename TRes::TTag offset)
    {
        return { TRes::OffsetKey(key.Key, offset), key.Priority };
    }

    template <class TRes>
    inline typename TTest<TRes>::TMyRes::TKey TTest<TRes>::TMyRes::MaxKey()
    {
        return { TRes::MaxKey(), std::numeric_limits<int>::max() };
    }

    template <class TRes>
    template <class ConsumerT>
    inline typename TTest<TRes>::TMyRes::TKey TTest<TRes>::TMyRes::ZeroKey(
        ConsumerT* consumer)
    {
        TKey key;
        key.Key = TRes::ZeroKey(consumer);
        TTest<TRes>::SetPriority(key.Priority, consumer);
        return key;
    }

    template <class TRes>
    inline void TTest<TRes>::TMyRes::ActivateKey(
        typename TTest<TRes>::TMyRes::TKey& key,
        typename TRes::TTag vtime)
    {
        TRes::ActivateKey(key.Key, vtime);
        // Keep key.Priority unchanged
    }

    namespace NSingleResource {
        using TRes = NShop::TSingleResource;
        using TMyConsumer = TTest<TRes>::TMyConsumer;
        using TMyShopState = TTest<TRes>::TMyShopState;
        using TMyScheduler = TTest<TRes>::TMyScheduler;
        using TMyQueue = TTest<TRes>::TMyQueue;
        using TQueuePtr = TTest<TRes>::TQueuePtr;

        template <class... TArgs>
        void Generate(TArgs&&... args)
        {
            TTest<TRes>::Generate(args...);
        }

        template <class... TArgs>
        void GenerateFront(TArgs&&... args)
        {
            TTest<TRes>::GenerateFront(args...);
        }

        template <class... TArgs>
        auto Schedule(TArgs&&... args)
        {
            return TTest<TRes>::Schedule(args...);
        }
    }

    Y_UNIT_TEST(StartLwtrace) {
        NLWTrace::StartLwtraceFromEnv();
    }

    Y_UNIT_TEST(Simple) {
        using namespace NSingleResource;

        TMyShopState state;
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABABAB");

        Generate(A, "50 50 50 50 50");
        Generate(B, "50 50 50 50 50");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABABABABAB");

        Generate(A, "20 20   20 20 20   20 20");
        Generate(B, "20 20   20 20 20   20 20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABABABABABABAB");

        Generate(A, "20 20   20 20 20   20 20");
        Generate(B, "50      50         50"   );
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABAABAAABA");

        Generate(A, "        100                100");
        Generate(B, "20 20   20 20 20    20 20     ");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABBBBBABB");

        TMyQueue* C;
        sched.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(2, 2)));

        Generate(A, "20     20     20     20     20     20");
        Generate(B, "20     20     20     20     20     20");
        Generate(C, "20 20  20 20  20 20  20 20  20 20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCCABCCABCCABCCABCCAB");

        sched.DeleteQueue("A");
        // sched.DeleteQueue("B"); // scheduler must delete queue be itself
    }

    Y_UNIT_TEST(DoubleActivation) {
        using namespace NSingleResource;

        TMyShopState state;
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "AB");

        A->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "AB");

        A->Activate();
        B->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AB");
    }

    Y_UNIT_TEST(SimpleLag) {
        using namespace NSingleResource;

        TMyShopState state;
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 0, TQueuePtr(D = new TMyQueue(1, 3)));
        sched.AddQueue("E", 0, TQueuePtr(E = new TMyQueue(1, 4)));

        Generate(A, "500 500 500"); // 25
        Generate(B, "500 500 500"); // 25
        Generate(C, "500 500 500"); // 25
        Generate(D, "500 500 500"); // 25
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 8), "ABCDABCD");
        Generate(E, "500"); // 100
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "EABCD");
    }

    Y_UNIT_TEST(Complex) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        TMyQueue* X;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 0, TQueuePtr(D = new TMyQueue(1, 3)));
        sched.AddQueue("E", 0, TQueuePtr(E = new TMyQueue(1, 4)));
        sched.AddQueue("X", 1, TQueuePtr(X = new TMyQueue(1, 5)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABABAB");

        Generate(A, "100 100 100");
        Generate(X, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AXAXAX");

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        Generate(E, "100 100 100");
        Generate(X, "100 100 100 100 100 100 100 100 100 100 100 100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDEXABCDEXABCDEXXXXXXXXXXXXX");
    }

    Y_UNIT_TEST(ComplexWithWeights) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(3, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(3, 3)));

        Generate(A, "100 100 100"); // 100
        Generate(B, "100 100 100"); // 100
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABABAB");

        Generate(C, "100 100 100"); // 100
        Generate(D, "100 100 100"); // 100
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCDCD");

        Generate(A, "100 100 100 100 100 100 100 100");
        Generate(C, "100 100 100 100 100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ACCCACCCACCAAAAA");

        Generate(B, "100 100 100 100 100 100 100 100");
        Generate(D, "100 100 100 100 100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "BDDDBDDDBDDBBBBB");

        Generate(A, "200 200 200 200 200 200 200 200 200 200 200");
        Generate(B, "200 200 200 200 200 200 200 200 200 200 200");
        Generate(C, "200 200 200 200 200 200 200 200 200 200 200");
        Generate(D, "200 200 200 200 200 200 200 200 200 200 200");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDCDCDABCDCDCDABCDCDCDABCDCDABABABABABABAB");

        Generate(A, "100 100 100 100 100 100 100 100 100 100 100");
        Generate(B, "100 100 100 100 100 100 100 100 100 100 100");
        Generate(C, "100 100 100 100 100 100 100 100 100 100 100");
        Generate(D, "100 100 100 100 100 100 100 100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDCDCDABCDCDCDABCDCDCDABCDCDABABABABABABAB");

        Generate(A, "50 50 50 50 50 50 50 50 50 50 50");
        Generate(B, "50 50 50 50 50 50 50 50 50 50 50");
        Generate(C, "50 50 50 50 50 50 50 50 50 50 50");
        Generate(D, "50 50 50 50 50 50 50 50 50 50 50");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDCDCDABCDCDCDABCDCDCDABCDCDABABABABABABAB");
    }

    Y_UNIT_TEST(OneQueueFrontPush) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue()));

        Generate(A, "10 20 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1, true), "A10");
        GenerateFront(A, "40");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "A40A20A30");
    }

    Y_UNIT_TEST(SimpleFrontPush) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "10 30 40");
        Generate(B, "10 20 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2, true), "A10B10");
        GenerateFront(A, "20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "A20B20A30B30A40");
    }

    Y_UNIT_TEST(SimpleFrontPushAll) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "10 30 40");
        Generate(B, " 5 30 40");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2, true), "A10B5");
        GenerateFront(A, "20");
        GenerateFront(B, "20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4, true), "B20A20B30A30");
        GenerateFront(A, "10 10");
        GenerateFront(B, "10 8");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "B8A10B10A10B40A40");
    }

    Y_UNIT_TEST(ComplexFrontPush) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "10 20");
        Generate(B, "10 30");
        Generate(C, "10 20");
        Generate(D, "10 20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4, true), "A10B10C10D10");
        GenerateFront(B, "20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "A20B20C20D20B30");
    }

    Y_UNIT_TEST(ComplexFrontPushAll) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "10 36");
        Generate(B, "10 34");
        Generate(C, "10 32");
        Generate(D, "10 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4, true), "A10B10C10D10");
        GenerateFront(A, "20");
        GenerateFront(B, "21");
        GenerateFront(C, "22");
        GenerateFront(D, "23");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "A20B21C22D23A36B34C32D30");
    }

    Y_UNIT_TEST(ComplexMultiplePush) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "5 5 5 5 5");
        Generate(B, "10 10");
        Generate(C, "10 10");
        Generate(D, "10 10");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDAABCDAA");
    }

    Y_UNIT_TEST(ComplexFrontMultiplePush) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "5");
        Generate(B, "10 10");
        Generate(C, "10 10");
        Generate(D, "10 10");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1, true), "A5");
        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1, true), "B10");
        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4, true), "C10D10A5A5");
        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1, true), "B10");
        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "C10D10A5A5");
    }

    Y_UNIT_TEST(ComplexCheckEmpty) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(1), true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(1), true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(1), true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(1), true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "A5");
    }

    Y_UNIT_TEST(SimpleFreeze) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(0);

        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "A");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "B");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "A");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "B");
    }

    Y_UNIT_TEST(FreezeSaveTagDiff) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));

        Generate(A, "100               100 100 100");
        Generate(B, "50          25 25 100 100 100");
        Generate(C, "100 100 100       100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 3), "ABC");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CC");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "BBABCABAB");
    }

    Y_UNIT_TEST(FreezeSaveTagDiffOnIdlePeriod) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));

        Generate(A, "100               100 100 100");
        Generate(B, "50          25 25 100 100 100");
        Generate(C, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 3), "ABC");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CC");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "BBABABAB");
    }

    Y_UNIT_TEST(FreezeSaveTagDiffOnSeveralIdlePeriods) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));

        Generate(A, "100               100 100 100");
        Generate(B, "50          25 25 100 100 100");
        Generate(C, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 3), "ABC");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CC");
        Generate(C, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CCC");
        Generate(C, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CCC");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "BBABABAB");
    }

    Y_UNIT_TEST(ComplexFreeze) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CD");
        state.Freeze(1);
        state.Unfreeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CD");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CD");
        state.Freeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CD");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        state.Freeze(0);
        state.Freeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(0);
        state.Unfreeze(1);
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        state.Freeze(0);
        state.Freeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "C");
        state.Freeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "D");
        state.Freeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "C");
        state.Freeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        state.Unfreeze(1);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "D");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");
    }

    Y_UNIT_TEST(ComplexLocalBusyPeriods) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "100 100");
        Generate(B, "100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
        for (int i = 0; i < 10; i++) {
            Generate(C, "50 50 50 40");
            Generate(D, "100   100  ");
            UNIT_ASSERT_STRINGS_EQUAL_C(Schedule(sched), "CDCCDC", i);
        }
        state.Unfreeze(0);
        Generate(C, "50 50 50");
        Generate(D, "50 50 50");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCDABCD");
    }

    Y_UNIT_TEST(ComplexGlobalBusyPeriods) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));

        for (int j = 0; j < 5; j++) {
            Generate(A, "100 100");
            Generate(B, "100 100");
            Generate(C, "100 100 100");
            Generate(D, "100 100 100");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
            state.Freeze(0);
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
            for (int i = 0; i < 5; i++) {
                Generate(C, "50 50 50 40");
                Generate(D, "100   100  ");
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCCDC");
            }
            state.Unfreeze(0);
            Generate(C, "50 50 25");
            Generate(D, "50 50 50");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCDABCD");
        }
    }

    Y_UNIT_TEST(VeryComplexFreeze) {
        using namespace NSingleResource;

        TMyShopState state(3);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        TMyQueue* F;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 3)));
        sched.AddQueue("E", 2, TQueuePtr(E = new TMyQueue(1, 4)));
        sched.AddQueue("F", 2, TQueuePtr(F = new TMyQueue(1, 5)));

        for (int j = 0; j < 5; j++) {
            Generate(A, "100 100");
            Generate(B, "100 100");
            Generate(C, "100 100 100");
            Generate(D, "100 100 100");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
            state.Freeze(0);
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
            for (int i = 0; i < 5; i++) {
                Generate(C, "50 50 50 40");
                Generate(D, "100   100  ");
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCCDC");
                Generate(E, "50 50 50 40");
                Generate(F, "100   100  ");
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "EFEEFE");
            }
            for (int i = 0; i < 5; i++) {
                Generate(C, "50 50 50 50");
                Generate(D, "100   100  ");
                Generate(E, "50 50 50 50 50 50");
                Generate(F, "100   100   100");
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 6), "CDEFCE");
                state.Freeze(1);
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 3), "EFE");
                state.Unfreeze(1);
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDEFCE");
            }
            state.Unfreeze(0);
            Generate(C, "50 50 55");
            Generate(D, "50 50 40");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCDABCD");
        }
    }

    Y_UNIT_TEST(SimplestClear) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));

        Generate(A, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AAA");

        Generate(A, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AAA");

        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
    }

    Y_UNIT_TEST(SimpleClear) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        sched.Attach(A);
        A->Activate();
        sched.Attach(B);
        B->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AB");
    }

    Y_UNIT_TEST(ComplexClear) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 1, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        sched.Attach(A);
        A->Activate();
        sched.Attach(B);
        B->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AB");
    }

    Y_UNIT_TEST(SimpleFreezeClear) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));

        Generate(A, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "A");

        state.Freeze(0);
        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        sched.Attach(A);
        A->Activate();
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AA");
    }

    Y_UNIT_TEST(EmptyFreezeClear) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));

        Generate(A, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AAA");

        state.Freeze(0);
        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
    }

    Y_UNIT_TEST(ComplexFreezeClear) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 1, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        state.Freeze(0);
        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        sched.Attach(A);
        sched.Attach(B);
        A->Activate(); // double activation should be ignored gracefully
        B->Activate();
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AB");
    }

    Y_UNIT_TEST(DeleteQueue) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100 100 100");
        Generate(B, "100 100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "AB");

        TMyQueue* C;
        sched.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(1, 2)));
        Generate(C, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CABC");

        sched.DeleteQueue("C");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        sched.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(1, 2)));
        Generate(C, "100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CABC");
    }

    Y_UNIT_TEST(SimpleActDeact) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        A->Deactivate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "B");

        A->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "A");
    }

    Y_UNIT_TEST(TotalActDeact) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        A->Deactivate();
        B->Deactivate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        B->Activate();
        A->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AB");
    }

    Y_UNIT_TEST(TotalActDeactReordered) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        B->Deactivate();
        A->Deactivate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        A->Activate();
        B->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AB");
    }

    Y_UNIT_TEST(ComplexActDeact) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        B->Deactivate();
        A->Deactivate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        sched.ActivateConsumers([=] (TMyConsumer* c) { return c == A? 0: NLazy::DoNotActivate; });
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "A");

        sched.ActivateConsumers([=] (TMyConsumer* c) { return c == B? 0: NLazy::DoNotActivate; });
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "B");
    }

    Y_UNIT_TEST(ComplexDeactDoubleAct) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", 0, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 6), "ABCDAB");

        A->Deactivate();
        B->Deactivate();
        C->Deactivate();
        D->Deactivate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        sched.ActivateConsumers([=] (TMyConsumer* c) { return c == A || c == D? 0: NLazy::DoNotActivate; });
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ADD");

        sched.ActivateConsumers([=] (TMyConsumer* c) { return c == B || c == C? 0: NLazy::DoNotActivate; });
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "BCC");
    }


    Y_UNIT_TEST(AttachIntoFreezableAfterDeact) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler sched(state);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(1, 2)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 6), "ABCABC");

        C->Deactivate();

        sched.AddQueue("D", 0, TQueuePtr(D = new TMyQueue(1, 3)));
        Generate(D, "100");

        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "DAB");

        C->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "C");
    }

    Y_UNIT_TEST(Random) {
        using namespace NSingleResource;

        for (int cycle = 0; cycle < 50; cycle++) {
            TMyShopState state(1);
            TMyScheduler sched(state);

            TVector<TQueuePtr> queues;
            for (int i = 0; i < 50; i++) {
                TQueuePtr queue(new TMyQueue(1, 0));
                queues.push_back(queue);
                sched.AddQueue(Sprintf("Q%03d", i), 0, queue);
            }

            for (int i = 0; i < 10000; i++) {
                if (RandomNumber<size_t>(10)) {
                    size_t i = RandomNumber<size_t>(queues.size());
                    TMyQueue* queue = queues[i].get();
                    switch (RandomNumber<size_t>(3)) {
                    case 0:
                        Generate(queue, "100");
                        queue->Activate();
                        break;
                    case 2:
                        queue->Deactivate();
                        break;
                    }
                } else {
                    Schedule(sched, 3);
                }
            }
        }
    }

    Y_UNIT_TEST(SimpleHierarchy) {
        using namespace NSingleResource;

        TMyShopState state(1);
        TMyScheduler schedR(state);

        TMyScheduler schedG(state, 1, 0);
        TMyQueue* A;
        TMyQueue* B;
        schedG.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        schedG.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        TMyScheduler schedH(state, 1, 0);
        TMyQueue* C;
        TMyQueue* D;
        schedH.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(1, 0)));
        schedH.AddQueue("D", 0, TQueuePtr(D = new TMyQueue(1, 1)));

        schedR.AddSubScheduler("G", &schedG);
        schedR.AddSubScheduler("H", &schedH);

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(schedR), "ACBDACBDACBD");
    }

    Y_UNIT_TEST(SimpleHierarchyFreezeSubTree) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler schedR(state);

        TMyScheduler schedG(state, 1, 0);
        TMyQueue* A;
        TMyQueue* B;
        schedG.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        schedG.AddQueue("B", 0, TQueuePtr(B = new TMyQueue(1, 1)));

        TMyScheduler schedH(state, 1, 0);
        TMyQueue* C;
        TMyQueue* D;
        schedH.AddQueue("C", 1, TQueuePtr(C = new TMyQueue(1, 0)));
        schedH.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 1)));

        schedR.AddSubScheduler("G", &schedG);
        schedR.AddSubScheduler("H", &schedH);

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(schedR, 4), "ACBD");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(schedR), "CDCD");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(schedR), "ABAB");
    }

    Y_UNIT_TEST(SimpleHierarchyFreezeLeafsOnly) {
        using namespace NSingleResource;

        TMyShopState state(2);
        TMyScheduler schedR(state);

        TMyScheduler schedG(state, 1, 0);
        TMyQueue* A;
        TMyQueue* B;
        schedG.AddQueue("A", 0, TQueuePtr(A = new TMyQueue(1, 0)));
        schedG.AddQueue("B", 1, TQueuePtr(B = new TMyQueue(1, 1)));

        TMyScheduler schedH(state, 1, 0);
        TMyQueue* C;
        TMyQueue* D;
        schedH.AddQueue("C", 0, TQueuePtr(C = new TMyQueue(1, 0)));
        schedH.AddQueue("D", 1, TQueuePtr(D = new TMyQueue(1, 1)));

        schedR.AddSubScheduler("G", &schedG);
        schedR.AddSubScheduler("H", &schedH);

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(schedR, 4), "ACBD");
        state.Freeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(schedR), "BDBD");
        state.Unfreeze(0);
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(schedR), "ACAC");
    }
}

template<>
void Out<NTestSuiteShopLazyScheduler::TTest<NShop::TSingleResource>::TMyRes::TKey>(
        IOutputStream& out,
        const NTestSuiteShopLazyScheduler::TTest<NShop::TSingleResource>::TMyRes::TKey& key)
{
    out << "<" << key.Key << "|" << key.Priority << ">";
}

template<>
void Out<NTestSuiteShopLazyScheduler::TTest<NShop::TPairDrf>::TMyRes::TKey>(
        IOutputStream& out,
        const NTestSuiteShopLazyScheduler::TTest<NShop::TPairDrf>::TMyRes::TKey& key)
{
    out << "<" << key.Key << "|" << key.Priority << ">";
}
