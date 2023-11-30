#include <ydb/library/actors/interconnect/ut/lib/node.h>
#include <ydb/library/actors/interconnect/ut/lib/ic_test_cluster.h>
#include <library/cpp/testing/unittest/registar.h>

TActorId MakeResponderServiceId(ui32 nodeId) {
    return TActorId(nodeId, TStringBuf("ResponderAct", 12));
}

class TArriveQueue {
    struct TArrivedItem {
        ui32 QueueId;
        ui32 Index;
        bool Success;
    };

    TMutex Lock;
    std::size_t Counter = 0;
    std::vector<TArrivedItem> Items;

public:
    TArriveQueue(size_t capacity)
        : Items(capacity)
    {}

    bool Done() const {
        with_lock (Lock) {
            return Counter == Items.size();
        }
    }

    void Push(ui64 cookie, bool success) {
        with_lock (Lock) {
            const size_t pos = Counter++;
            TArrivedItem item{.QueueId = static_cast<ui32>(cookie >> 32), .Index = static_cast<ui32>(cookie & 0xffff'ffff),
                .Success = success};
            memcpy(&Items[pos], &item, sizeof(TArrivedItem));
        }
    }

    void Check() {
        struct TPerQueueState {
            std::vector<ui32> Ok, Error;
        };
        std::unordered_map<ui32, TPerQueueState> state;
        for (const TArrivedItem& item : Items) {
            auto& st = state[item.QueueId];
            auto& v = item.Success ? st.Ok : st.Error;
            v.push_back(item.Index);
        }
        for (const auto& [queueId, st] : state) {
            ui32 expected = 0;
            for (const ui32 index : st.Ok) {
                Y_ABORT_UNLESS(index == expected);
                ++expected;
            }
            for (const ui32 index : st.Error) {
                Y_ABORT_UNLESS(index == expected);
                ++expected;
            }
            if (st.Error.size()) {
                Cerr << "Error.size# " << st.Error.size() << Endl;
            }
        }
    }
};

class TResponder : public TActor<TResponder> {
    TArriveQueue& ArriveQueue;

public:
    TResponder(TArriveQueue& arriveQueue)
        : TActor(&TResponder::StateFunc)
        , ArriveQueue(arriveQueue)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvPing, Handle);
    )

    void Handle(TEvents::TEvPing::TPtr ev) {
        ArriveQueue.Push(ev->Cookie, true);
    }
};

class TSender : public TActor<TSender> {
    TArriveQueue& ArriveQueue;

public:
    TSender(TArriveQueue& arriveQueue)
        : TActor(&TThis::StateFunc)
        , ArriveQueue(arriveQueue)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvUndelivered, Handle);
    )

    void Handle(TEvents::TEvUndelivered::TPtr ev) {
        ArriveQueue.Push(ev->Cookie, false);
    }
};

void SenderThread(TMutex& lock, TActorSystem *as, ui32 nodeId, ui32 queueId, ui32 count, TArriveQueue& arriveQueue) {
    const TActorId sender = as->Register(new TSender(arriveQueue));
    with_lock(lock) {}
    const TActorId target = MakeResponderServiceId(nodeId);
    for (ui32 i = 0; i < count; ++i) {
        const ui32 flags = IEventHandle::FlagTrackDelivery;
        as->Send(new IEventHandle(TEvents::THelloWorld::Ping, flags, target, sender, nullptr, ((ui64)queueId << 32) | i));
    }
}

void RaceTestIter(ui32 numThreads, ui32 count) {
    TPortManager portman;
    THashMap<ui32, ui16> nodeToPort;
    const ui32 numNodes = 6; // total
    const ui32 numDynamicNodes = 3;
    for (ui32 i = 1; i <= numNodes; ++i) {
        nodeToPort.emplace(i, portman.GetPort());
    }

    NMonitoring::TDynamicCounterPtr counters = new NMonitoring::TDynamicCounters;
    std::list<TNode> nodes;
    for (ui32 i = 1; i <= numNodes; ++i) {
        nodes.emplace_back(i, numNodes, nodeToPort, "127.1.0.0", counters->GetSubgroup("nodeId", TStringBuilder() << i),
            TDuration::Seconds(10), TChannelsConfig(), numDynamicNodes, numThreads);
    }

    const ui32 numSenders = 10;
    TArriveQueue arriveQueue(numSenders * numNodes * (numNodes - 1) * count);
    for (TNode& node : nodes) {
        node.RegisterServiceActor(MakeResponderServiceId(node.GetActorSystem()->NodeId), new TResponder(arriveQueue));
    }

    TMutex lock;
    std::list<TThread> threads;
    ui32 queueId = 0;
    with_lock(lock) {
        for (TNode& from : nodes) {
            for (ui32 toId = 1; toId <= numNodes; ++toId) {
                if (toId == from.GetActorSystem()->NodeId) {
                    continue;
                }
                for (ui32 i = 0; i < numSenders; ++i) {
                    threads.emplace_back([=, &lock, &from, &arriveQueue] {
                        SenderThread(lock, from.GetActorSystem(), toId, queueId, count, arriveQueue);
                    });
                    ++queueId;
                }
            }
        }
        for (auto& thread : threads) {
            thread.Start();
        }
    }
    for (auto& thread : threads) {
        thread.Join();
    }

    for (THPTimer timer; !arriveQueue.Done(); TDuration::MilliSeconds(10)) {
        Y_ABORT_UNLESS(timer.Passed() < 10);
    }

    nodes.clear();
    arriveQueue.Check();
}

Y_UNIT_TEST_SUITE(DynamicProxy) {
    Y_UNIT_TEST(RaceCheck1) {
        for (ui32 iteration = 0; iteration < 100; ++iteration) {
            RaceTestIter(1 + iteration % 5, 1);
        }
    }
    Y_UNIT_TEST(RaceCheck10) {
        for (ui32 iteration = 0; iteration < 100; ++iteration) {
            RaceTestIter(1 + iteration % 5, 10);
        }
    }
}
