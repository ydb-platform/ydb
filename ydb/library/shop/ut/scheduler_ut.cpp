#include <ydb/library/shop/scheduler.h>

#include <util/generic/deque.h>
#include <util/random/random.h>
#include <util/string/vector.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ShopScheduler) {
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
        };

        class TMyQueue;
        using TMySchedulable = TSchedulable<typename TMyRes::TCost>;
        using TMyConsumer = TConsumer<TMyRes>;
        using TMyFreezable = TFreezable<TMyRes>;

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
            bool AllowEmpty = false;
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

            void PushTask(TMyTask* task)
            {
                if (Empty()) { // Scheduler must be notified on first task in queue
                    this->Activate();
                }
                task->Queue = this;
                Tasks.push_back(task);
            }

            void PushTaskFront(TMyTask* task)
            {
                if (Empty()) { // Scheduler must be notified on first task in queue
                    this->Activate();
                }
                task->Queue = this;
                Tasks.push_front(task);
            }
        public: // Interface for scheduler
            TMySchedulable* PopSchedulable() override
            {
                if (AllowEmpty && Tasks.empty()) {
                    return nullptr;
                }
                UNIT_ASSERT(!Tasks.empty());
                TMyTask* task = Tasks.front();
                Tasks.pop_front();
                return task;
            }

            bool Empty() const override
            {
                return Tasks.empty();
            }
        };

        ///////////////////////////////////////////////////////////////////////////////

        using TQueuePtr = std::shared_ptr<TMyQueue>;
        using TFreezablePtr = std::shared_ptr<TMyFreezable>;

        class TMyScheduler: public TScheduler<TMyRes> {
        public:
            THashMap<TString, TFreezablePtr> Freezables;
            THashMap<TString, TQueuePtr> Queues;
            bool AllowEmpty;
            int Priority;
        public:
            explicit TMyScheduler(bool allowEmpty, TWeight w = 1, int priority = 0)
                : AllowEmpty(allowEmpty)
                , Priority(priority)
            {
                this->SetWeight(w);
            }

            explicit TMyScheduler(TWeight w = 1, int priority = 0)
                : TMyScheduler(false, w, priority)
            {}

            ~TMyScheduler()
            {
                this->UpdateCounters();
            }

            void AddFreezable(const TString& name, const TFreezablePtr& freezable)
            {
                freezable->SetName(name);
                freezable->SetScheduler(this);
                Freezables.emplace(name, freezable);
            }

            void DeleteFreezable(const TString& name)
            {
                auto iter = Freezables.find(name);
                if (iter != Freezables.end()) {
                    TMyFreezable* freezable = iter->second.Get();
                    freezable->Deactivate();
                    Freezables.erase(iter);
                }
            }

            void AddQueue(const TString& name, TMyFreezable* freezable, const TQueuePtr& queue)
            {
                queue->SetName(name);
                queue->SetScheduler(this);
                queue->SetFreezable(freezable);
                queue->AllowEmpty = AllowEmpty;
                Queues.emplace(name, queue);
            }

            void AddSubScheduler(const TString& name, TMyFreezable* freezable, TMyScheduler* scheduler)
            {
                scheduler->SetName(name);
                scheduler->SetScheduler(this);
                scheduler->SetFreezable(freezable);
                scheduler->AllowEmpty = AllowEmpty;
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
            while (count-- && !sched.Empty()) {
                TMyTask* task = static_cast<TMyTask*>(sched.PopSchedulable());
                if (sched.AllowEmpty && !task) {
                    break;
                }
                ss << task->Queue->GetName();
                if (printcost) {
                    ss << task->Cost;
                }
                delete task;
            }
            if (count != size_t(-1)) {
                UNIT_ASSERT(sched.Empty());
            }
            return ss.Str();
        }
    };

    ///////////////////////////////////////////////////////////////////////////////

    template <class TRes>
    template <class ConsumerT>
    inline void TTest<TRes>::TMyRes::SetKey(
        typename TTest<TRes>::TMyRes::TKey& key,
        ConsumerT* consumer)
    {
        TRes::SetKey(key.Key, consumer);
        if (auto queue = dynamic_cast<TTest<TRes>::TMyQueue*>(consumer)) {
            key.Priority = queue->Priority;
        } else if (auto sched = dynamic_cast<TTest<TRes>::TMyScheduler*>(consumer)) {
            key.Priority = sched->Priority;
        }
    }

    template <class TRes>
    inline typename TTest<TRes>::TMyRes::TKey TTest<TRes>::TMyRes::OffsetKey(
        typename TTest<TRes>::TMyRes::TKey key,
        typename TRes::TTag offset)
    {
        return { TRes::OffsetKey(key.Key, offset), key.Priority };
    }

    namespace NSingleResource {
        using TRes = NShop::TSingleResource;
        using TMyConsumer = TTest<TRes>::TMyConsumer;
        using TMyFreezable = TTest<TRes>::TMyFreezable;
        using TMyScheduler = TTest<TRes>::TMyScheduler;
        using TMyQueue = TTest<TRes>::TMyQueue;
        using TQueuePtr = TTest<TRes>::TQueuePtr;
        using TFreezablePtr = TTest<TRes>::TFreezablePtr;

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

    namespace NPairDrf {
        using TRes = NShop::TPairDrf;
        using TMyConsumer = TTest<TRes>::TMyConsumer;
        using TMyFreezable = TTest<TRes>::TMyFreezable;
        using TMyScheduler = TTest<TRes>::TMyScheduler;
        using TMyQueue = TTest<TRes>::TMyQueue;
        using TQueuePtr = TTest<TRes>::TQueuePtr;
        using TFreezablePtr = TTest<TRes>::TFreezablePtr;

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

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
        sched.AddQueue("C", G, TQueuePtr(C = new TMyQueue(2, 2)));

        Generate(A, "20     20     20     20     20     20");
        Generate(B, "20     20     20     20     20     20");
        Generate(C, "20 20  20 20  20 20  20 20  20 20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCCABCCABCCABCCABCCAB");

        sched.DeleteQueue("A");
        // sched.DeleteQueue("B"); // scheduler must delete queue be itself
    }

//    Y_UNIT_TEST(CompileDrf) {
//        using namespace NPairDrf;

//        TMyScheduler sched;
//        TMyFreezable* G;
//        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
//        TMyQueue* A;
//        TMyQueue* B;
//        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
//        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
//    }

    Y_UNIT_TEST(DoubleActivation) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", G, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", G, TQueuePtr(D = new TMyQueue(1, 3)));
        sched.AddQueue("E", G, TQueuePtr(E = new TMyQueue(1, 4)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        TMyQueue* X;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", G, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", G, TQueuePtr(D = new TMyQueue(1, 3)));
        sched.AddQueue("E", G, TQueuePtr(E = new TMyQueue(1, 4)));
        sched.AddQueue("X", H, TQueuePtr(X = new TMyQueue(1, 5)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(3, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(3, 3)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue()));

        Generate(A, "10 20 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1, true), "A10");
        GenerateFront(A, "40");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "A40A20A30");
    }

    Y_UNIT_TEST(SimpleFrontPush) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "10 30 40");
        Generate(B, "10 20 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2, true), "A10B10");
        GenerateFront(A, "20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, size_t(-1), true), "A20B20A30B30A40");
    }

    Y_UNIT_TEST(SimpleFrontPushAll) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "5 5 5 5 5");
        Generate(B, "10 10");
        Generate(C, "10 10");
        Generate(D, "10 10");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDAABCDAA");
    }

    Y_UNIT_TEST(ComplexFrontMultiplePush) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "A");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "B");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "A");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "B");
    }

    Y_UNIT_TEST(ComplexFreeze) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CD");
        H->Freeze();
        H->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CD");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CD");
        H->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        H->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "CD");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        G->Freeze();
        H->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        G->Unfreeze();
        H->Unfreeze();
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");

        Generate(A, "100 100 100 100");
        Generate(B, "100 100 100 100");
        Generate(C, "100 100 100 100");
        Generate(D, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        G->Freeze();
        H->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        H->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "C");
        H->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        H->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "D");
        H->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        H->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "C");
        H->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
        H->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "D");
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ABCDABAB");
    }

    Y_UNIT_TEST(ComplexLocalBusyPeriods) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));

        Generate(A, "100 100");
        Generate(B, "100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
        G->Freeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
        for (int i = 0; i < 10; i++) {
            Generate(C, "50 50 50 40");
            Generate(D, "100   100  ");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCCDC");
        }
        G->Unfreeze();
        Generate(C, "50 50 50");
        Generate(D, "50 50 50");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCDABCD");
    }

    Y_UNIT_TEST(ComplexGlobalBusyPeriods) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));

        for (int j = 0; j < 5; j++) {
            Generate(A, "100 100");
            Generate(B, "100 100");
            Generate(C, "100 100 100");
            Generate(D, "100 100 100");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
            G->Freeze();
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CDCD");
            for (int i = 0; i < 5; i++) {
                Generate(C, "50 50 50 40");
                Generate(D, "100   100  ");
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCCDC");
            }
            G->Unfreeze();
            Generate(C, "50 50 25");
            Generate(D, "50 50 50");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCDABCD");
        }
    }

    Y_UNIT_TEST(VeryComplexFreeze) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));
        TMyFreezable* K;
        sched.AddFreezable("K", TFreezablePtr(K = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        TMyQueue* F;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 3)));
        sched.AddQueue("E", K, TQueuePtr(E = new TMyQueue(1, 4)));
        sched.AddQueue("F", K, TQueuePtr(F = new TMyQueue(1, 5)));

        for (int j = 0; j < 5; j++) {
            Generate(A, "100 100");
            Generate(B, "100 100");
            Generate(C, "100 100 100");
            Generate(D, "100 100 100");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABCD");
            G->Freeze();
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
                H->Freeze();
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 3), "EFE");
                H->Unfreeze();
                UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDEFCE");
            }
            G->Unfreeze();
            Generate(C, "50 50 55");
            Generate(D, "50 50 40");
            UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CDCDABCD");
        }
    }

    Y_UNIT_TEST(SimplestClear) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));

        Generate(A, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AAA");

        Generate(A, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AAA");

        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
    }

    Y_UNIT_TEST(SimpleClear) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        A->Activate();
        B->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AB");
    }

    Y_UNIT_TEST(ComplexClear) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", H, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        A->Activate();
        B->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AB");
    }

    Y_UNIT_TEST(SimpleFreezeClear) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));

        Generate(A, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 1), "A");

        G->Freeze();
        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AA");
    }

    Y_UNIT_TEST(EmptyFreezeClear) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));

        Generate(A, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "AAA");

        G->Freeze();
        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");
    }

    Y_UNIT_TEST(ComplexFreezeClear) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyFreezable* H;
        sched.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", H, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        G->Freeze();
        sched.Clear();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        A->Activate(); // double activation should be ignored gracefully
        B->Activate();
        G->Unfreeze();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "BA");
    }

    Y_UNIT_TEST(DeleteQueue) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100 100 100");
        Generate(B, "100 100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 2), "AB");

        TMyQueue* C;
        sched.AddQueue("C", G, TQueuePtr(C = new TMyQueue(1, 2)));
        Generate(C, "100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "CABC");

        sched.DeleteQueue("C");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        sched.AddQueue("C", G, TQueuePtr(C = new TMyQueue(1, 2)));
        Generate(C, "100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "CABC");
    }

    Y_UNIT_TEST(SimpleActDeact) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

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

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 4), "ABAB");

        B->Deactivate();
        A->Deactivate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "");

        G->ActivateConsumers([=] (TMyConsumer* c) { return c == A; });
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "A");

        G->ActivateConsumers([=] (TMyConsumer* c) { return c == B; });
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "B");
    }

    Y_UNIT_TEST(ComplexDeactDoubleAct) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", G, TQueuePtr(C = new TMyQueue(1, 2)));
        sched.AddQueue("D", G, TQueuePtr(D = new TMyQueue(1, 3)));

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

        G->ActivateConsumers([=] (TMyConsumer* c) { return c == A || c == D; });
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "ADD");

        G->ActivateConsumers([=] (TMyConsumer* c) { return c == B || c == C; });
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "BCC");
    }


    Y_UNIT_TEST(AttachIntoFreezableAfterDeact) {
        using namespace NSingleResource;

        TMyScheduler sched;
        TMyFreezable* G;
        sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        sched.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        sched.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));
        sched.AddQueue("C", G, TQueuePtr(C = new TMyQueue(1, 2)));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched, 6), "ABCABC");

        C->Deactivate();

        sched.AddQueue("D", G, TQueuePtr(D = new TMyQueue(1, 3)));
        Generate(D, "100");

        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "DAB");

        C->Activate();
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(sched), "C");
    }

    Y_UNIT_TEST(Random) {
        using namespace NSingleResource;

        for (int cycle = 0; cycle < 50; cycle++) {
            TMyScheduler sched(true);
            TMyFreezable* G;
            sched.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));

            TVector<TQueuePtr> queues;
            for (int i = 0; i < 50; i++) {
                TQueuePtr queue(new TMyQueue(1, 0));
                queues.push_back(queue);
                sched.AddQueue(Sprintf("Q%03d", i), G, queue);
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

        TMyScheduler schedR; // must be destructed last

        TMyScheduler schedG(1, 0);
        TMyFreezable* G;
        schedG.AddFreezable("G", TFreezablePtr(G = new TMyFreezable()));
        TMyQueue* A;
        TMyQueue* B;
        schedG.AddQueue("A", G, TQueuePtr(A = new TMyQueue(1, 0)));
        schedG.AddQueue("B", G, TQueuePtr(B = new TMyQueue(1, 1)));

        TMyScheduler schedH(1, 0);
        TMyFreezable* H;
        schedH.AddFreezable("H", TFreezablePtr(H = new TMyFreezable()));
        TMyQueue* C;
        TMyQueue* D;
        schedH.AddQueue("C", H, TQueuePtr(C = new TMyQueue(1, 0)));
        schedH.AddQueue("D", H, TQueuePtr(D = new TMyQueue(1, 1)));

        TMyFreezable* R;
        schedR.AddFreezable("R", TFreezablePtr(R = new TMyFreezable()));
        schedR.AddSubScheduler("G", R, &schedG);
        schedR.AddSubScheduler("H", R, &schedH);

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(schedR), "ACBDACBDACBD");
    }
}

template<>
void Out<NTestSuiteShopScheduler::TTest<NShop::TSingleResource>::TMyRes::TKey>(
        IOutputStream& out,
        const NTestSuiteShopScheduler::TTest<NShop::TSingleResource>::TMyRes::TKey& key)
{
    out << "<" << key.Key << "|" << key.Priority << ">";
}

template<>
void Out<NTestSuiteShopScheduler::TTest<NShop::TPairDrf>::TMyRes::TKey>(
        IOutputStream& out,
        const NTestSuiteShopScheduler::TTest<NShop::TPairDrf>::TMyRes::TKey& key)
{
    out << "<" << key.Key << "|" << key.Priority << ">";
}
