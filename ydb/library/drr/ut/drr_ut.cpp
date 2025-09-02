#include <ydb/library/drr/drr.h>
#include <library/cpp/threading/future/legacy_future.h>

#include <util/random/random.h>
#include <util/generic/list.h>
#include <util/string/vector.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(SchedulingDRR) {
    using namespace NScheduling;

    class TMyQueue;

    class TMyTask {
    public:
        TUCost Cost;
        TMyQueue* Queue; // Needed only for test
    public:
        TMyTask(TUCost cost)
            : Cost(cost)
            , Queue(nullptr)
        {}

        TUCost GetCost() const
        {
            return Cost;
        }
    };

    class TMyQueue: public TDRRQueue {
    public:
        typedef TMyTask TTask;
    public:
        typedef TList<TMyTask*> TTasks;
        TString Name;
        TTasks Tasks;
    public: // Interface for clients
        TMyQueue(const TString& name, TWeight w = 1, TUCost maxBurst = 0)
            : TDRRQueue(w, maxBurst)
            , Name(name)
        {}

        ~TMyQueue()
        {
            for (TTasks::iterator i = Tasks.begin(), e = Tasks.end(); i != e; ++i) {
                delete *i;
            }
        }

        void PushTask(TMyTask* task)
        {
            task->Queue = this;
            if (Tasks.empty()) {
                // Scheduler must be notified on first task in queue
                if (GetScheduler()) {
                    GetScheduler()->ActivateQueue(this);
                }
            }
            Tasks.push_back(task);
        }

        void PushTaskFront(TMyTask* task)
        {
            task->Queue = this;
            if (Tasks.empty()) {
                // Scheduler must be notified on first task in queue
                if (GetScheduler()) {
                    GetScheduler()->ActivateQueue(this);
                }
            }
            Tasks.push_front(task);
            if (GetScheduler()) {
                GetScheduler()->DropCache(this);
            }
        }
    public: // Interface for scheduler
        void OnSchedulerAttach()
        {
            Y_ASSERT(GetScheduler() != nullptr);
            if (!Tasks.empty()) {
                GetScheduler()->ActivateQueue(this);
            }
        }

        TTask* PeekTask()
        {
            UNIT_ASSERT(!Tasks.empty());
            return Tasks.front();
        }

        void PopTask()
        {
            UNIT_ASSERT(!Tasks.empty());
            Tasks.pop_front();
        }

        bool Empty() const
        {
            return Tasks.empty();
        }
    };

    void Generate(TMyQueue* queue, const TString& tasks)
    {
        TVector<TString> v = SplitString(tasks, " ");
        for (size_t i = 0; i < v.size(); i++) {
            queue->PushTask(new TMyTask(FromString<TUCost>(v[i])));
        }
    }

    void GenerateFront(TMyQueue* queue, const TString& tasks)
    {
        TVector<TString> v = SplitString(tasks, " ");
        for (size_t i = 0; i < v.size(); i++) {
            queue->PushTaskFront(new TMyTask(FromString<TUCost>(v[i])));
        }
    }

    template <class TDRR>
    TString Schedule(TDRR& drr, size_t count = size_t(-1), bool printcost = false) {
        TStringStream ss;
        while (count--) {
            TMyTask* task = drr.PeekTask();
            if (!task)
                break;
            drr.PopTask();
            ss << task->Queue->Name;
            if (printcost) {
                ss << task->Cost;
            }
            delete task;
        }
        if (count != size_t(-1)) {
            UNIT_ASSERT(drr.PeekTask() == nullptr);
        }
        return ss.Str();
    }

    typedef std::shared_ptr<TMyQueue> TQueuePtr;

    Y_UNIT_TEST(SimpleDRR) {
        TDRRScheduler<TMyQueue> drr(100);

        TMyQueue* A;
        TMyQueue* B;
        drr.AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        drr.AddQueue("B", TQueuePtr(B = new TMyQueue("B")));

        Generate(A, "100 100 100"); // 50
        Generate(B, "100 100 100"); // 50
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "ABABAB");

        Generate(A, "50 50 50 50 50"); // 50
        Generate(B, "50 50 50 50 50"); // 50
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "ABABABABAB");

        Generate(A, "20 20   20 20 20   20 20"); // 50
        Generate(B, "20 20   20 20 20   20 20"); // 50
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "AABBAAABBBAABB");

        Generate(A, "20 20   20 20 20   20 20"); // 50
        Generate(B, "50      50         50"   ); // 50
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "AABAAABAAB");

        Generate(A, "        100                100"); // 50
        Generate(B, "20 20   20 20 20    20 20     "); // 50
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "BBABBBBBA");

        TMyQueue* C;
        drr.AddQueue("C", TQueuePtr(C = new TMyQueue("C", 2)));

        Generate(A, "20     20        20     20 20     20"); // 25
        Generate(B, "20     20        20     20 20     20"); // 25
        Generate(C, "20 20  20 20 20  20 20  20 20 20  20"); // 50
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "ABCCABCCCABCCAABBCCCABC");

        drr.GetQueue("B"); // just to instantiate GetQueue() function from template

        drr.DeleteQueue("A");
        // DRR.DeleteQueue("B"); // scheduler must delete queue be itself
    }

    Y_UNIT_TEST(SimpleDRRLag) {
        TDRRScheduler<TMyQueue> drr(100);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        drr.AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        drr.AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        drr.AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        drr.AddQueue("D", TQueuePtr(D = new TMyQueue("D")));
        drr.AddQueue("E", TQueuePtr(E = new TMyQueue("E")));

        Generate(A, "500 500 500"); // 25
        Generate(B, "500 500 500"); // 25
        Generate(C, "500 500 500"); // 25
        Generate(D, "500 500 500"); // 25
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 8), "ABCDABCD");
        Generate(E, "500"); // 100
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "ABCED");
    }

    Y_UNIT_TEST(SimpleDRRWithOneBursty) {
        TDRRScheduler<TMyQueue> drr(100);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        drr.AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        drr.AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        drr.AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        drr.AddQueue("D", TQueuePtr(D = new TMyQueue("D")));
        drr.AddQueue("E", TQueuePtr(E = new TMyQueue("E", 1, 500)));

        Generate(A, "500 500 500"); // 25
        Generate(B, "500 500 500"); // 25
        Generate(C, "500 500 500"); // 25
        Generate(D, "500 500 500"); // 25
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 8), "ABCDABCD");
        Generate(E, "500"); // 100
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "EABCD");

        // The same test to avoid effect of first message after add
        Generate(A, "500 500 500"); // 25
        Generate(B, "500 500 500"); // 25
        Generate(C, "500 500 500"); // 25
        Generate(D, "500 500 500"); // 25
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 8), "ABCDABCD");
        Generate(E, "500"); // 20
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "EABCD");
    }

    Y_UNIT_TEST(SimpleDRRWithAllBursty) {
        TDRRScheduler<TMyQueue> drr(100);

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        drr.AddQueue("A", TQueuePtr(A = new TMyQueue("A", 1, 500)));
        drr.AddQueue("B", TQueuePtr(B = new TMyQueue("B", 1, 500)));
        drr.AddQueue("C", TQueuePtr(C = new TMyQueue("C", 1, 500)));
        drr.AddQueue("D", TQueuePtr(D = new TMyQueue("D", 1, 500)));
        drr.AddQueue("E", TQueuePtr(E = new TMyQueue("E", 1, 500)));

        Generate(A, "500 500 500"); // 25
        Generate(B, "500 500 500"); // 25
        Generate(C, "500 500 500"); // 25
        Generate(D, "500 500 500"); // 25
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 8), "ABCDABCD");
        Generate(E, "500"); // 20
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "EABCD");
    }

    Y_UNIT_TEST(DoubleDRR) {
        typedef TDRRScheduler<TMyQueue> TInnerDRR;
        typedef std::shared_ptr<TInnerDRR> TInnerDRRPtr;
        typedef TDRRScheduler<TInnerDRR> TOuterDRR;
        TOuterDRR drr(200);

        TInnerDRR* G;
        drr.AddQueue("G", TInnerDRRPtr(G = new TInnerDRR(200)));
        TInnerDRR* H;
        drr.AddQueue("H", TInnerDRRPtr(H = new TInnerDRR(200)));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        TMyQueue* E;
        TMyQueue* X;
        G->AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        G->AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        G->AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        G->AddQueue("D", TQueuePtr(D = new TMyQueue("D")));
        G->AddQueue("E", TQueuePtr(E = new TMyQueue("E")));
        H->AddQueue("X", TQueuePtr(X = new TMyQueue("X")));

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "ABABAB");

        Generate(A, "100 100 100");
        Generate(X, "100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "AXAXAX");

        Generate(A, "100 100 100");
        Generate(B, "100 100 100");
        Generate(C, "100 100 100");
        Generate(D, "100 100 100");
        Generate(E, "100 100 100");
        Generate(X, "100 100 100 100 100 100 100 100 100 100 100 100 100 100 100");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "AXBXCXDXEXAXBXCXDXEXAXBXCXDXEX");
    }

    Y_UNIT_TEST(DoubleDRRWithWeights) {
        typedef TDRRScheduler<TMyQueue> TInnerDRR;
        typedef std::shared_ptr<TInnerDRR> TInnerDRRPtr;
        typedef TDRRScheduler<TInnerDRR> TOuterDRR;
        TOuterDRR drr(200);

        TInnerDRR* G;
        drr.AddQueue("G", TInnerDRRPtr(G = new TInnerDRR(200, 1)));
        TInnerDRR* H;
        drr.AddQueue("H", TInnerDRRPtr(H = new TInnerDRR(200, 3)));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        G->AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        G->AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        H->AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        H->AddQueue("D", TQueuePtr(D = new TMyQueue("D")));

        Generate(A, "100 100 100"); // 100
        Generate(B, "100 100 100"); // 100
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "ABABAB");

        Generate(C, "100 100 100"); // 100
        Generate(D, "100 100 100"); // 100
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "CDCDCD");

        Generate(A, "100 100 100 100 100 100 100 100"); // 50
        Generate(C, "100 100 100 100 100 100 100 100"); // 150
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "CACCCACCCACAAAAA");

        Generate(B, "100 100 100 100 100 100 100 100"); // 50
        Generate(D, "100 100 100 100 100 100 100 100"); // 150
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "DBDDDBDDDBDBBBBB");

        Generate(A, "200 200 200 200 200 200 200 200 200 200 200"); // 25
        Generate(B, "200 200 200 200 200 200 200 200 200 200 200"); // 25
        Generate(C, "200 200 200 200 200 200 200 200 200 200 200"); // 75
        Generate(D, "200 200 200 200 200 200 200 200 200 200 200"); // 75
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "CDACDCBDCDACDCBDCDACDCBDCDACDBABABABABABABAB");

        Generate(A, "100 100 100 100 100 100 100 100 100 100 100"); // 25
        Generate(B, "100 100 100 100 100 100 100 100 100 100 100"); // 25
        Generate(C, "100 100 100 100 100 100 100 100 100 100 100"); // 75
        Generate(D, "100 100 100 100 100 100 100 100 100 100 100"); // 75
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "CADCDBCDCADCDBCDCADCDBCDCADCDBABABABABABABAB");

        Generate(A, "50 50 50 50 50 50 50 50 50 50 50"); // 25
        Generate(B, "50 50 50 50 50 50 50 50 50 50 50"); // 25
        Generate(C, "50 50 50 50 50 50 50 50 50 50 50"); // 75
        Generate(D, "50 50 50 50 50 50 50 50 50 50 50"); // 75
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "ACCDADCCBDDCBCDDACCDADCCBDDCBDAABBAABBAABBAB");

        Generate(A, "25 25 25 25 25 25 25 25 25 25 25"); // 25
        Generate(B, "25 25 25 25 25 25 25 25 25 25 25"); // 25
        Generate(C, "25 25 25 25 25 25 25 25 25 25 25"); // 75
        Generate(D, "25 25 25 25 25 25 25 25 25 25 25"); // 75
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr), "AACCCCDDAADDCCCCBBDDDDCCBBCDDDAAAABBBBAAABBB");
    }

    Y_UNIT_TEST(OneQueueDRRFrontPush) {
        TDRRScheduler<TMyQueue> drr(100);

        TMyQueue* A;
        drr.AddQueue("A", TQueuePtr(A = new TMyQueue("A")));

        Generate(A, "10 20 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 1, true), "A10");

        GenerateFront(A, "40");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(-1), true), "A40A20A30");
    }

    Y_UNIT_TEST(SimpleDRRFrontPush) {
        TDRRScheduler<TMyQueue> drr(10);

        TMyQueue* A;
        TMyQueue* B;
        drr.AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        drr.AddQueue("B", TQueuePtr(B = new TMyQueue("B")));

        Generate(A, "10 30 40");
        Generate(B, "10 20 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 2, true), "A10B10");

        GenerateFront(A, "20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(-1), true), "A20B20A30B30A40");
    }

    Y_UNIT_TEST(SimpleDRRFrontPushAll) {
        TDRRScheduler<TMyQueue> drr(10);

        TMyQueue* A;
        TMyQueue* B;
        drr.AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        drr.AddQueue("B", TQueuePtr(B = new TMyQueue("B")));

        Generate(A, "10 30");
        Generate(B, "10 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 2, true), "A10B10");

        GenerateFront(A, "20");
        GenerateFront(B, "20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(-1), true), "A20B20A30B30");
    }

    Y_UNIT_TEST(DoubleDRRFrontPush) {
        typedef TDRRScheduler<TMyQueue> TInnerDRR;
        typedef std::shared_ptr<TInnerDRR> TInnerDRRPtr;
        typedef TDRRScheduler<TInnerDRR> TOuterDRR;
        TOuterDRR drr(10);

        TInnerDRR* G;
        drr.AddQueue("G", TInnerDRRPtr(G = new TInnerDRR(10)));
        TInnerDRR* H;
        drr.AddQueue("H", TInnerDRRPtr(H = new TInnerDRR(10)));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        G->AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        G->AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        H->AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        H->AddQueue("D", TQueuePtr(D = new TMyQueue("D")));

        Generate(A, "10 20");
        Generate(B, "10 30");
        Generate(C, "10 20");
        Generate(D, "10 20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 4, true), "A10C10B10D10");

        GenerateFront(B, "20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(-1), true), "A20C20B20D20B30");
    }

    Y_UNIT_TEST(DoubleDRRFrontPushAll) {
        typedef TDRRScheduler<TMyQueue> TInnerDRR;
        typedef std::shared_ptr<TInnerDRR> TInnerDRRPtr;
        typedef TDRRScheduler<TInnerDRR> TOuterDRR;
        TOuterDRR drr(10);

        TInnerDRR* G;
        drr.AddQueue("G", TInnerDRRPtr(G = new TInnerDRR(10)));
        TInnerDRR* H;
        drr.AddQueue("H", TInnerDRRPtr(H = new TInnerDRR(10)));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        G->AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        G->AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        H->AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        H->AddQueue("D", TQueuePtr(D = new TMyQueue("D")));

        Generate(A, "10 30");
        Generate(B, "10 30");
        Generate(C, "10 30");
        Generate(D, "10 30");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 4, true), "A10C10B10D10");

        GenerateFront(A, "20");
        GenerateFront(B, "20");
        GenerateFront(C, "20");
        GenerateFront(D, "20");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(-1), true), "A20C20B20D20A30C30B30D30");
    }

    Y_UNIT_TEST(DoubleDRRMultiplePush) {
        typedef TDRRScheduler<TMyQueue> TInnerDRR;
        typedef std::shared_ptr<TInnerDRR> TInnerDRRPtr;
        typedef TDRRScheduler<TInnerDRR> TOuterDRR;
        TOuterDRR drr(40);

        TInnerDRR* G;
        drr.AddQueue("G", TInnerDRRPtr(G = new TInnerDRR(20)));
        TInnerDRR* H;
        drr.AddQueue("H", TInnerDRRPtr(H = new TInnerDRR(20)));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        G->AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        G->AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        H->AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        H->AddQueue("D", TQueuePtr(D = new TMyQueue("D")));

        Generate(A, "5 5 5 5 5");
        Generate(B, "10 10");
        Generate(C, "10 10");
        Generate(D, "10 10");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(-1), true), "A5A5B10C10D10A5A5B10C10D10A5");
    }

    Y_UNIT_TEST(DoubleDRRFrontMultiplePush) {
        typedef TDRRScheduler<TMyQueue> TInnerDRR;
        typedef std::shared_ptr<TInnerDRR> TInnerDRRPtr;
        typedef TDRRScheduler<TInnerDRR> TOuterDRR;
        TOuterDRR drr(40);

        TInnerDRR* G;
        drr.AddQueue("G", TInnerDRRPtr(G = new TInnerDRR(20)));
        TInnerDRR* H;
        drr.AddQueue("H", TInnerDRRPtr(H = new TInnerDRR(20)));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        G->AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        G->AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        H->AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        H->AddQueue("D", TQueuePtr(D = new TMyQueue("D")));
//        Cerr << "A\t" << (ui64)A << Endl;
//        Cerr << "B\t" << (ui64)B << Endl;
//        Cerr << "C\t" << (ui64)C << Endl;
//        Cerr << "D\t" << (ui64)D << Endl;
//        Cerr << "G\t" << (ui64)G << Endl;
//        Cerr << "H\t" << (ui64)H << Endl;


        Generate(A, "5");
        Generate(B, "10 10");
        Generate(C, "10 10");
        Generate(D, "10 10");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 1, true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 1, true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 4, true), "B10C10D10A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, 1, true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(-1), true), "B10C10D10A5");
    }

    Y_UNIT_TEST(DoubleDRRCheckEmpty) {
        typedef TDRRScheduler<TMyQueue> TInnerDRR;
        typedef std::shared_ptr<TInnerDRR> TInnerDRRPtr;
        typedef TDRRScheduler<TInnerDRR> TOuterDRR;
        TOuterDRR drr(40);

        TInnerDRR* G;
        drr.AddQueue("G", TInnerDRRPtr(G = new TInnerDRR(20)));
        TInnerDRR* H;
        drr.AddQueue("H", TInnerDRRPtr(H = new TInnerDRR(20)));

        TMyQueue* A;
        TMyQueue* B;
        TMyQueue* C;
        TMyQueue* D;
        G->AddQueue("A", TQueuePtr(A = new TMyQueue("A")));
        G->AddQueue("B", TQueuePtr(B = new TMyQueue("B")));
        H->AddQueue("C", TQueuePtr(C = new TMyQueue("C")));
        H->AddQueue("D", TQueuePtr(D = new TMyQueue("D")));

        Generate(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(1), true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(1), true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(1), true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(1), true), "A5");

        GenerateFront(A, "5");
        UNIT_ASSERT_STRINGS_EQUAL(Schedule(drr, size_t(-1), true), "A5");
    }
    // TODO(serxa): add test for weight update
}
