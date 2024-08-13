#include <ydb/core/blobstorage/ut_blobstorage/lib/env.h>
#include <ydb/core/blobstorage/ut_blobstorage/lib/common.h>
#include <ydb/core/blobstorage/dsproxy/group_sessions.h>

#include <util/generic/hash_set.h>
#include <util/stream/null.h>

#include "ut_helpers.h"

#define Ctest Cnull

Y_UNIT_TEST_SUITE(Acceleration) {
    using TFlowRecord = TIntrusivePtr<NBackpressure::TFlowRecord>;
    using TQueueId = NKikimrBlobStorage::EVDiskQueueId;

    struct TDiskDelay {
        TWeightedRandom<TDuration> Delays;
        TDuration Max;
        TString Tag;

        TDiskDelay(TDuration delay = TDuration::Zero(), TString tag = "")
            : Max(delay)
            , Tag(tag)
        {
            Delays.AddValue(delay, 1);
        }

        TDiskDelay(TDuration min, ui64 minWeight, TDuration max, ui64 maxWeight, TString tag = "")
            : Max(max)
            , Tag(tag)
        {
            Delays.AddValue(min, minWeight);
            Delays.AddValue(max, maxWeight);
        }

        TDiskDelay(const TDiskDelay&) = default;
        TDiskDelay(TDiskDelay&&) = default;
        TDiskDelay& operator=(const TDiskDelay&) = default;
        TDiskDelay& operator=(TDiskDelay&&) = default;

        TDuration GetRandom() {
            return Delays.GetRandom();
        }
    };

    struct TEvDelayedMessageWrapper : public TEventLocal<TEvDelayedMessageWrapper, TEvBlobStorage::EvDelayedMessageWrapper> {
    public:
        std::unique_ptr<IEventHandle> Event;

        TEvDelayedMessageWrapper(std::unique_ptr<IEventHandle>& ev)
            : Event(ev.release())
        {}
    };

    struct TVDiskDelayEmulator {
        TVDiskDelayEmulator(const std::shared_ptr<TEnvironmentSetup>& env)
            : Env(env)
        {}

        using TFlowKey = std::pair<ui32, TQueueId>;  // { nodeId, queueId }

        std::shared_ptr<TEnvironmentSetup> Env;
        TActorId Edge;
        // assuming there is only one disk per node
        std::unordered_map<TFlowKey, TFlowRecord> FlowRecords;

        std::unordered_map<ui32, TDiskDelay> DelayByNode;
        std::deque<TDiskDelay> DelayByResponseOrder;
        TDiskDelay DefaultDelay = TDuration::Seconds(1);
        bool LogUnwrap = false;

        using TEventHandler = std::function<bool(std::unique_ptr<IEventHandle>&)>;

        std::unordered_map<ui32, TEventHandler> EventHandlers;

        void AddHandler(ui32 eventType, TEventHandler handler) {
            EventHandlers[eventType] = handler;
        }

        bool Filter(ui32/* nodeId*/, std::unique_ptr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvDelayedMessageWrapper::EventType) {
                std::unique_ptr<IEventHandle> delayedMsg(std::move(ev));
                ev.reset(delayedMsg->Get<TEvDelayedMessageWrapper>()->Event.release());
                if (LogUnwrap) {
                    Ctest << TAppData::TimeProvider->Now() << " Unwrap " << ev->ToString() << Endl;
                }
                return true;
            }
            
            ui32 type = ev->GetTypeRewrite();
            auto it = EventHandlers.find(type);
            if (it != EventHandlers.end() && it->second) {
                return (it->second)(ev);
            }
            return true;
        }

        TDuration GetMsgDelay(ui32 vdiskNodeId) {
            TDiskDelay& delay = DefaultDelay;
            auto it = DelayByNode.find(vdiskNodeId);
            if (it == DelayByNode.end()) {
                if (!DelayByResponseOrder.empty()) {
                    delay = DelayByResponseOrder.front();
                    DelayByResponseOrder.pop_front();
                }
                DelayByNode[vdiskNodeId] = delay;
            } else {
                delay = it->second;
            }
            TDuration rand = delay.GetRandom();
            return rand;
        }

        TDuration DelayMsg(std::unique_ptr<IEventHandle>& ev) {
            TDuration delay = GetMsgDelay(ev->Sender.NodeId());

            Env->Runtime->WrapInActorContext(Edge, [&] {
                TActivationContext::Schedule(delay, new IEventHandle(
                        ev->Sender,
                        ev->Recipient,
                        new TEvDelayedMessageWrapper(ev))
                );
            });
            return delay;
        }

        void SetDelayByResponseOrder(const std::deque<TDiskDelay>& delays) {
            DelayByResponseOrder = delays;
            DelayByNode = {};
        }
    };

    struct TDelayer {
        std::shared_ptr<TVDiskDelayEmulator> VDiskDelayEmulator;

        bool operator()(ui32 nodeId, std::unique_ptr<IEventHandle>& ev) {
            return VDiskDelayEmulator->Filter(nodeId, ev);
        }
    };

    struct TestCtx {
        TestCtx(const TBlobStorageGroupType& erasure, float slowDiskThreshold, float delayMultiplier)
            : NodeCount(erasure.BlobSubgroupSize() + 1)
            , Erasure(erasure)
            , Env(new TEnvironmentSetup({
                .NodeCount = NodeCount,
                .Erasure = erasure,
                .LocationGenerator = [this](ui32 nodeId) { return LocationGenerator(nodeId); },
                .SlowDiskThreshold = slowDiskThreshold,
                .VDiskPredictedDelayMultiplier = delayMultiplier,
            }))
            , VDiskDelayEmulator(new TVDiskDelayEmulator(Env))
        {}
    
        TNodeLocation LocationGenerator(ui32 nodeId) {
            if (Erasure.BlobSubgroupSize() == 9) {
                if (nodeId == NodeCount) {
                    return TNodeLocation{"4", "1", "1", "1"};
                }
                return TNodeLocation{
                    std::to_string((nodeId - 1) / 3),
                    "1",
                    std::to_string((nodeId - 1) % 3),
                    "0"
                };
            } else {
                if (nodeId == NodeCount) {
                    return TNodeLocation{"2", "1", "1", "1"};
                }
                return TNodeLocation{"1", "1", std::to_string(nodeId), "0"};
            }
        }

        void Initialize() {
            Env->CreateBoxAndPool(1, 1);
            Env->Sim(TDuration::Minutes(1));

            NKikimrBlobStorage::TBaseConfig base = Env->FetchBaseConfig();
            UNIT_ASSERT_VALUES_EQUAL(base.GroupSize(), 1);
            const auto& group = base.GetGroup(0);
            GroupId = group.GetGroupId();

            Edge = Env->Runtime->AllocateEdgeActor(NodeCount);
            VDiskDelayEmulator->Edge = Edge;

            std::unordered_map<ui32, ui32> OrderNumberToNodeId;

            for (ui32 orderNum = 0; orderNum < group.VSlotIdSize(); ++orderNum) {
                OrderNumberToNodeId[orderNum] = group.GetVSlotId(orderNum).GetNodeId();
            }

            Env->Runtime->WrapInActorContext(Edge, [&] {
                SendToBSProxy(Edge, GroupId, new TEvBlobStorage::TEvStatus(TInstant::Max()));
            });
            auto res = Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvStatusResult>(Edge, false, TInstant::Max());

            Env->Runtime->FilterFunction = TDelayer{ .VDiskDelayEmulator = VDiskDelayEmulator };

            for (const TQueueId& queueId : { TQueueId::PutTabletLog, TQueueId::GetFastRead, TQueueId::PutAsyncBlob,
                    TQueueId::GetAsyncRead }) {
                Ctest << "Send TEvGetQueuesInfo " << queueId << Endl;
                Env->Runtime->WrapInActorContext(Edge, [&] {
                    SendToBSProxy(Edge, GroupId, new TEvGetQueuesInfo(queueId));
                });
                auto res = Env->WaitForEdgeActorEvent<TEvQueuesInfo>(Edge, false, TInstant::Max());
                Ctest << "Get TEvQueuesInfo " << res->Get()->ToString() << Endl;

                for (ui32 orderNum = 0; orderNum < res->Get()->Queues.size(); ++orderNum) {
                    const std::optional<TEvQueuesInfo::TQueueInfo>& queue = res->Get()->Queues[orderNum];
                    if (queue) {
                        Y_ABORT_UNLESS(queue->FlowRecord);
                        queue->FlowRecord->SetPredictedDelayNs(VDiskDelayEmulator->DefaultDelay.Max.NanoSeconds());
                        VDiskDelayEmulator->FlowRecords[{ OrderNumberToNodeId[orderNum], queueId }] = queue->FlowRecord;
                    }
                }
            }
        }

        ~TestCtx() {
            Env->Runtime->FilterFunction = {};
        }

        ui32 NodeCount;
        TBlobStorageGroupType Erasure;
        std::shared_ptr<TEnvironmentSetup> Env;

        ui32 GroupId;
        TActorId Edge;
        std::shared_ptr<TVDiskDelayEmulator> VDiskDelayEmulator;
    };

    #define ADD_DSPROXY_MESSAGE_PRINTER(MsgType)                                                                        \
        ctx.VDiskDelayEmulator->AddHandler(MsgType::EventType, [&](std::unique_ptr<IEventHandle>& ev) {                 \
                if (ev->Recipient.NodeId() == ctx.NodeCount) {                                                          \
                    Ctest << TAppData::TimeProvider->Now() << " Send "#MsgType": " << ev->Sender.ToString() << " " <<   \
                            ev->Recipient.ToString() << ev->Get<MsgType>()->ToString() << Endl;                         \
                }                                                                                                       \
                return true;                                                                                            \
            }                                                                                                           \
        )

    void TestAcceleratePut(const TBlobStorageGroupType& erasure, ui32 slowDisksNum,
            NKikimrBlobStorage::EPutHandleClass handleClass, TDuration fastDelay,
            TDuration slowDelay, TDuration initDelay, TDuration waitTime,
            float delayMultiplier) {
        ui32 initialRequests = 100;
        float slowDiskThreshold = 2;
        TDiskDelay fastDiskDelay = TDiskDelay(fastDelay);
        TDiskDelay slowDiskDelay = TDiskDelay(slowDelay);
        TDiskDelay initDiskDelay = TDiskDelay(initDelay);

        for (ui32 fastDisksNum = 0; fastDisksNum < erasure.BlobSubgroupSize() - 2; ++fastDisksNum) {
            Ctest << "fastDisksNum# " << fastDisksNum << Endl;
            TestCtx ctx(erasure, slowDiskThreshold, delayMultiplier);
            ctx.VDiskDelayEmulator->DefaultDelay = initDiskDelay;
            ctx.Initialize();

            TString data = MakeData(1024);
            auto put = [&](TLogoBlobID blobId) {
                ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                    SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()), handleClass);
                });
                auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(
                        ctx.Edge, false, TAppData::TimeProvider->Now() + waitTime);
                UNIT_ASSERT_C(res, "fastDisksNum# " << fastDisksNum);
                UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            };

            bool verboseHandlers = false;
            ctx.VDiskDelayEmulator->AddHandler(TEvBlobStorage::TEvVPutResult::EventType, [&](std::unique_ptr<IEventHandle>& ev) {
                ui32 nodeId = ev->Sender.NodeId();
                if (nodeId < ctx.NodeCount) {
                    TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetVDiskID());
                    TLogoBlobID partId = LogoBlobIDFromLogoBlobID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetBlobID());
                    TDuration delay = ctx.VDiskDelayEmulator->DelayMsg(ev);
                    if (verboseHandlers) {
                        Ctest << TAppData::TimeProvider->Now() << " TEvVPutResult: vdiskId# " << vdiskId.ToString() <<
                                " partId# " << partId.ToString() << " nodeId# " << nodeId << ", delay " << delay << Endl;
                    }
                    return false;
                }
                return true;
            });

            for (ui32 i = 0; i < initialRequests; ++i) {
                put(TLogoBlobID(1, 1, 1, 1, data.size(), 123 + i));
            }

            ctx.Env->Sim(slowDelay);

            std::deque<TDiskDelay> delayByResponseOrder;
            for (ui32 i = 0; i < erasure.BlobSubgroupSize(); ++i) {
                if (i >= fastDisksNum  && i < fastDisksNum + slowDisksNum) {
                    delayByResponseOrder.push_back(slowDiskDelay);
                } else {
                    delayByResponseOrder.push_back(fastDiskDelay);
                }
            }
            ctx.VDiskDelayEmulator->SetDelayByResponseOrder(delayByResponseOrder);

            ctx.VDiskDelayEmulator->LogUnwrap = true;
            verboseHandlers = true;
            ADD_DSPROXY_MESSAGE_PRINTER(TEvBlobStorage::TEvVPut);
            put(TLogoBlobID(1, 1, 1, 1, data.size(), 1));

        }
    }

    void TestAccelerateGet(const TBlobStorageGroupType& erasure, ui32 slowDisksNum,
            NKikimrBlobStorage::EGetHandleClass handleClass, TDuration fastDelay,
            TDuration slowDelay, TDuration initDelay, TDuration waitTime,
            float delayMultiplier) {
        ui32 initialRequests = 100;
        float slowDiskThreshold = 2;
        TDiskDelay fastDiskDelay = TDiskDelay(fastDelay);
        TDiskDelay slowDiskDelay = TDiskDelay(slowDelay);
        TDiskDelay initDiskDelay = TDiskDelay(initDelay);

        for (ui32 fastDisksNum = 0; fastDisksNum < erasure.BlobSubgroupSize() - 2; ++fastDisksNum) {
            Ctest << "fastDisksNum# " << fastDisksNum << Endl;
            TestCtx ctx(erasure, slowDiskThreshold, delayMultiplier);
            ctx.VDiskDelayEmulator->DefaultDelay = initDiskDelay;
            ctx.Initialize();

            bool verboseHandlers = false;
            ctx.VDiskDelayEmulator->AddHandler(TEvBlobStorage::TEvVGetResult::EventType, [&](std::unique_ptr<IEventHandle>& ev) {
                ui32 nodeId = ev->Sender.NodeId();
                if (nodeId < ctx.NodeCount) {
                    TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetVDiskID());
                    TLogoBlobID partId = LogoBlobIDFromLogoBlobID(
                            ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetResult(0).GetBlobID());
                    TDuration delay = ctx.VDiskDelayEmulator->DelayMsg(ev);
                    if (verboseHandlers) {
                        Ctest << TAppData::TimeProvider->Now() << " TEvVGetResult: vdiskId# " << vdiskId.ToString() <<
                                " partId# " << partId.ToString() << " nodeId# " << nodeId << ", delay " << delay << Endl;
                    }
                    return false;
                }
                return true;
            });

            TString data = MakeData(1024);
            auto putAndGet = [&](TLogoBlobID blobId) {
                ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                    SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
                });
                auto putRes = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
                UNIT_ASSERT_VALUES_EQUAL(putRes->Get()->Status, NKikimrProto::OK);

                ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                    SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvGet(blobId, 0, data.size(), TInstant::Max(), handleClass));
                });
                auto getRes = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(ctx.Edge, false, TAppData::TimeProvider->Now() + waitTime);
                UNIT_ASSERT_C(getRes, "fastDisksNum# " << fastDisksNum);
                UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->Status, NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(getRes->Get()->Responses[0].Status, NKikimrProto::OK);
            };

            for (ui32 i = 0; i < initialRequests; ++i) {
                putAndGet(TLogoBlobID(1, 1, 1, 1, data.size(), 123 + i));
            }
            ctx.Env->Sim(slowDelay);

            std::deque<TDiskDelay> delayByResponseOrder;
            for (ui32 i = 0; i < erasure.BlobSubgroupSize(); ++i) {
                if (i >= fastDisksNum  && i < fastDisksNum + slowDisksNum) {
                    delayByResponseOrder.push_back(slowDiskDelay);
                } else {
                    delayByResponseOrder.push_back(fastDiskDelay);
                }
            }
            ctx.VDiskDelayEmulator->SetDelayByResponseOrder(delayByResponseOrder);

            ctx.VDiskDelayEmulator->LogUnwrap = true;
            verboseHandlers = true;
            ADD_DSPROXY_MESSAGE_PRINTER(TEvBlobStorage::TEvVGet);
            putAndGet(TLogoBlobID(1, 1, 1, 1, data.size(), 2));
        }
    }

    using TTestThresholdRequestSender = std::function<void(TestCtx&, ui32)>;

    void TestThresholdSendPutRequests(TestCtx& ctx, ui32 requests) {
        ui64 cookie = 1;

        for (ui32 i = 0; i < requests; ++i) {
            TString data = "Test";
            TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 1, data.size(), ++cookie);

            Ctest << " ------------------- Send TEvPut# " << i << " ------------------- " << Endl;
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
            });
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }
    }

    void TestThresholdSendGetRequests(TestCtx& ctx, ui32 requests) {
        ui64 cookie = 1;
        std::vector<TLogoBlobID> blobs;
        TString data = MakeData(1024);

        for (ui32 i = 0; i < requests; ++i) {
            TLogoBlobID blobId = TLogoBlobID(1, 1, 1, 1, data.size(), ++cookie);
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvPut(blobId, data, TInstant::Max()));
            });

            blobs.push_back(blobId);
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvPutResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
        }

        for (const auto& blobId : blobs) {
            ctx.Env->Runtime->WrapInActorContext(ctx.Edge, [&] {
                SendToBSProxy(ctx.Edge, ctx.GroupId, new TEvBlobStorage::TEvGet(blobId, 0, data.size(), TInstant::Max(),
                        NKikimrBlobStorage::AsyncRead));
            });
            auto res = ctx.Env->WaitForEdgeActorEvent<TEvBlobStorage::TEvGetResult>(ctx.Edge, false, TInstant::Max());
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Status, NKikimrProto::OK);
            UNIT_ASSERT_VALUES_EQUAL(res->Get()->Responses[0].Status, NKikimrProto::OK);
        }
    }

    void TestThreshold(const TBlobStorageGroupType& erasure, ui32 slowDisks, bool delayPuts, bool delayGets,
            TTestThresholdRequestSender sendRequests) {
        float delayMultiplier = 1;
        float slowDiskThreshold = 1.2;
        TDiskDelay fastDiskDelay = TDiskDelay(TDuration::Seconds(0.1), 10, TDuration::Seconds(1), 1, "fast");
        TDiskDelay slowDiskDelay = TDiskDelay(TDuration::Seconds(1.5), "slow");

        ui32 requests = 1000;
        
        TestCtx ctx(erasure, slowDiskThreshold, delayMultiplier);
        ctx.VDiskDelayEmulator->DefaultDelay = fastDiskDelay;
        ui32 groupSize = erasure.BlobSubgroupSize();

        std::vector<bool> nodeIsSlow(groupSize, true);
        std::vector<ui32> vputsByNode(groupSize, 0);
    
        for (ui32 i = 0; i < groupSize; ++i) {
            bool isSlow = (i % 3 == 0 && i / 3 < slowDisks);
            ctx.VDiskDelayEmulator->DelayByNode[i + 1] = isSlow ? slowDiskDelay : fastDiskDelay;
            nodeIsSlow[i] = isSlow;
        }

        ctx.Initialize();
        ctx.VDiskDelayEmulator->LogUnwrap = true;
        
        if (delayPuts) {
            ctx.VDiskDelayEmulator->AddHandler(TEvBlobStorage::TEvVPutResult::EventType, [&](std::unique_ptr<IEventHandle>& ev) {
                ui32 nodeId = ev->Sender.NodeId();
                if (nodeId < ctx.NodeCount) {
                    TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetVDiskID());
                    TLogoBlobID partId = LogoBlobIDFromLogoBlobID(ev->Get<TEvBlobStorage::TEvVPutResult>()->Record.GetBlobID());
                    TDuration delay = ctx.VDiskDelayEmulator->DelayMsg(ev);
                    Ctest << TAppData::TimeProvider->Now() << " TEvVPutResult: vdiskId# " << vdiskId.ToString() <<
                            " partId# " << partId.ToString() << " nodeId# " << nodeId << ", delay " << delay << Endl;
                    ++vputsByNode[nodeId - 1];
                    return false;
                }
                return true;
            });
            ADD_DSPROXY_MESSAGE_PRINTER(TEvBlobStorage::TEvVPut);
        }

        if (delayGets) {
            ctx.VDiskDelayEmulator->AddHandler(TEvBlobStorage::TEvVGetResult::EventType, [&](std::unique_ptr<IEventHandle>& ev) {
                ui32 nodeId = ev->Sender.NodeId();
                if (nodeId < ctx.NodeCount) {
                    TVDiskID vdiskId = VDiskIDFromVDiskID(ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetVDiskID());
                    TLogoBlobID partId = LogoBlobIDFromLogoBlobID(
                            ev->Get<TEvBlobStorage::TEvVGetResult>()->Record.GetResult(0).GetBlobID());
                    TDuration delay = ctx.VDiskDelayEmulator->DelayMsg(ev);
                    Ctest << TAppData::TimeProvider->Now() << " TEvVGetResult: vdiskId# " << vdiskId.ToString() <<
                            " partId# " << partId.ToString() << " nodeId# " << nodeId << ", delay " << delay << Endl;
                    ++vputsByNode[nodeId - 1];
                    return false;
                }
                return true;
            });
            ADD_DSPROXY_MESSAGE_PRINTER(TEvBlobStorage::TEvVGet);
        }

        sendRequests(ctx, requests);

        ui32 slowNodesCount = 0;
        ui32 slowNodesRequests = 0;
        ui32 fastNodesCount = 0;
        ui32 fastNodesRequests = 0;

        TStringStream str;

        str << "VPUTS BY NODE: ";
        for (ui32 i = 0; i < groupSize; ++i) {
            str << "{ nodeId# " << i << " isSlow# " << nodeIsSlow[i] << ' ' << vputsByNode[i] << "}, ";
            if (nodeIsSlow[i]) {
                ++slowNodesCount;
                slowNodesRequests += vputsByNode[i];
            } else {
                ++fastNodesCount;
                fastNodesRequests += vputsByNode[i];
            }
        }
        Ctest << str.Str() << Endl;

        double slowNodeRequestsAvg = 1. * slowNodesRequests / slowNodesCount;
        double fastNodeRequestsAvg = 1. * fastNodesRequests / fastNodesCount;

        UNIT_ASSERT_LE_C(slowNodeRequestsAvg, fastNodeRequestsAvg / 3, str.Str());
    }

    void TestThresholdPut(const TBlobStorageGroupType& erasure, ui32 slowDisks) {
        TestThreshold(erasure, slowDisks, true, false, TestThresholdSendPutRequests);
    }

    void TestThresholdGet(const TBlobStorageGroupType& erasure, ui32 slowDisks) {
        TestThreshold(erasure, slowDisks, false, true, TestThresholdSendGetRequests);
    }

    void TestDelayMultiplierPut(const TBlobStorageGroupType& erasure, ui32 slowDisks) {
        TestAcceleratePut(erasure, slowDisks, NKikimrBlobStorage::AsyncBlob, TDuration::Seconds(0.9),
                TDuration::Seconds(2), TDuration::Seconds(1), TDuration::Seconds(1.95), 0.8);
    }

    void TestDelayMultiplierGet(const TBlobStorageGroupType& erasure, ui32 slowDisks) {
        TestAccelerateGet(erasure, slowDisks, NKikimrBlobStorage::AsyncRead, TDuration::Seconds(0.9),
                TDuration::Seconds(2
                ), TDuration::Seconds(1), TDuration::Seconds(1.95), 0.8);
    }

    #define TEST_ACCELERATE(erasure, method, handleClass, slowDisks)                                                    \
    Y_UNIT_TEST(TestAcceleration##erasure##method##handleClass##slowDisks##Slow) {                                      \
        TestAccelerate##method(TBlobStorageGroupType::Erasure##erasure, slowDisks, NKikimrBlobStorage::handleClass,     \
                TDuration::Seconds(1), TDuration::Seconds(5), TDuration::Seconds(1), TDuration::Seconds(4), 1);         \
    }

    TEST_ACCELERATE(Mirror3dc, Put, AsyncBlob, 1);
//    TEST_ACCELERATE(Mirror3of4, Put, AsyncBlob, 1);
    TEST_ACCELERATE(4Plus2Block, Put, AsyncBlob, 1);

    TEST_ACCELERATE(Mirror3dc, Put, AsyncBlob, 2);
//    TEST_ACCELERATE(Mirror3of4, Put, AsyncBlob, 2);
    TEST_ACCELERATE(4Plus2Block, Put, AsyncBlob, 2);

    TEST_ACCELERATE(Mirror3dc, Get, AsyncRead, 1);
//    TEST_ACCELERATE(Mirror3of4, Get, AsyncRead, 1);
    TEST_ACCELERATE(4Plus2Block, Get, AsyncRead, 1);

    TEST_ACCELERATE(Mirror3dc, Get, AsyncRead, 2);
//    TEST_ACCELERATE(Mirror3of4, Get, AsyncRead, 2);
    TEST_ACCELERATE(4Plus2Block, Get, AsyncRead, 2);

    #define TEST_ACCELERATE_PARAMS(param, method, erasure, slowDisks)               \
    Y_UNIT_TEST(Test##param##method##erasure##slowDisks##Slow) {                    \
        Test##param##method(TBlobStorageGroupType::Erasure##erasure, slowDisks);    \
    }

//    TEST_ACCELERATE_PARAMS(Threshold, Put, Mirror3dc, 1);
    TEST_ACCELERATE_PARAMS(Threshold, Put, 4Plus2Block, 1);

//    TEST_ACCELERATE_PARAMS(Threshold, Put, Mirror3dc, 2);
//    TEST_ACCELERATE_PARAMS(Threshold, Put, 4Plus2Block, 2);

//    TEST_ACCELERATE_PARAMS(Threshold, Get, Mirror3dc, 1);
    TEST_ACCELERATE_PARAMS(Threshold, Get, 4Plus2Block, 1);

//    TEST_ACCELERATE_PARAMS(Threshold, Get, Mirror3dc, 2);
//    TEST_ACCELERATE_PARAMS(Threshold, Get, 4Plus2Block, 2);

    // TODO(serg-belyakov): fix all muted tests

    TEST_ACCELERATE_PARAMS(DelayMultiplier, Put, Mirror3dc, 1);
    TEST_ACCELERATE_PARAMS(DelayMultiplier, Put, 4Plus2Block, 1);

    TEST_ACCELERATE_PARAMS(DelayMultiplier, Put, Mirror3dc, 2);
    TEST_ACCELERATE_PARAMS(DelayMultiplier, Put, 4Plus2Block, 2);

    TEST_ACCELERATE_PARAMS(DelayMultiplier, Get, Mirror3dc, 1);
    TEST_ACCELERATE_PARAMS(DelayMultiplier, Get, 4Plus2Block, 1);

    TEST_ACCELERATE_PARAMS(DelayMultiplier, Get, Mirror3dc, 2);
    TEST_ACCELERATE_PARAMS(DelayMultiplier, Get, 4Plus2Block, 2);

    #undef TEST_ACCELERATE
    #undef TEST_ACCELERATE_PARAMS
    #undef PRINT_DSPROXY_MESSAGE
}
