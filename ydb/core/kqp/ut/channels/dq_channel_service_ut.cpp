#include "dq_channel_service.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/runtime/dq_channel_service_impl.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <library/cpp/threading/mux_event/mux_event.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/library/yql/dq/actors/dq.h>
#include <util/random/random.h>

using namespace NKikimr::NKqp;
using namespace NYql::NDq;

using namespace NYdb;
using namespace NYdb::NTable;

template<>
void Out<NYql::NDq::EDqFillLevel>(IOutputStream& os, const NYql::NDq::EDqFillLevel l) {
    os << static_cast<ui32>(l);
}

struct TEvTestPrivate {
    enum ERole {
        Producer,
        Consumer,
    };

    enum EEv {
        EvStart = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvFinished,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvStart : public NActors::TEventLocal<TEvStart, EvStart> {
        TEvStart(NActors::TActorId peerId) : PeerId(peerId) {}
        NActors::TActorId PeerId;
    };

    struct TEvFinished : public NActors::TEventLocal<TEvFinished, EvFinished> {
        TEvFinished(ERole role, bool error) : Role(role), Error(error) {}
        ERole Role;
        bool Error;
    };
};

struct TWorkerSettings {
    int StartDelayMs = 10;
    int MessageCount = 0;
    int MinMessageSize = 10;
    int MaxMessageSize = 10000;
    bool EarlyFinish = false;
};

struct TFailureSettings {
    int Data = 0;
    int Ack = 0;
    int Update = 0;
    int Discovery = 0;
};

class TWorkerActor : public NActors::TActor<TWorkerActor> {
public:
    TWorkerActor(std::shared_ptr<IDqChannelService> service, TEvTestPrivate::ERole role, ui32 channelId, const TWorkerSettings& settings)
        : TActor(&TThis::StateFunc)
        , Service(service)
        , Role(role)
        , ChannelId(channelId)
        , Settings(settings)
    {}

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
            hFunc(TEvTestPrivate::TEvStart, HandleStart);
            hFunc(TEvDqCompute::TEvResumeExecution, HandleResume);
            hFunc(NYql::NDq::TEvDq::TEvAbortExecution, HandleAbort);
        }
    }

    void Run() {
        if (!Started) {
            switch (Role) {
                case TEvTestPrivate::ERole::Producer: {
                    TChannelFullInfo info(ChannelId, SelfId(), PeerId, 0, 1, TCollectStatsLevel::None);
                    Buffer = Service->GetOutputBuffer(info, nullptr, nullptr);
                    break;
                }
                case TEvTestPrivate::ERole::Consumer: {
                    TChannelFullInfo info(ChannelId, PeerId, SelfId(), 0, 1, TCollectStatsLevel::None);
                    Buffer = Service->GetInputBuffer(info, nullptr);
                    break;
                }
            }
            Started = true;
        }

        switch (Role) {
            case TEvTestPrivate::ERole::Producer: {
                if (Buffer->IsFinished()) {
                    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, "TEST FINISHED SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
                    Send(RunnerId, new TEvTestPrivate::TEvFinished(Role, false));
                    PassAway();
                    return;
                }
                while (Buffer->GetFillLevel() == EDqFillLevel::NoLimit && MessageIndex <= Settings.MessageCount) {
                    if (MessageIndex == Settings.MessageCount) {
                        Buffer->SendFinish();
                    } else {
                        auto bytes = Settings.MinMessageSize + RandomNumber<ui64>(Settings.MaxMessageSize - Settings.MinMessageSize);
                        Buffer->Push(TDataChunk(NYql::TChunkedBuffer(TString(bytes, 'a')), 1, false));
                    }
                    MessageIndex++;
                }
                break;
            }
            case TEvTestPrivate::ERole::Consumer: {
                TDataChunk data;
                while (MessageIndex < Settings.MessageCount && Buffer->Pop(data)) {
                    MessageIndex++;
                }
                if (Settings.EarlyFinish && MessageIndex == Settings.MessageCount) {
                    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, "TEST EARLY FINISH SelfId=" << SelfId() << ", ChannelId=" << ChannelId);
                    Buffer->EarlyFinish();
                    MessageIndex++;
                }
                if (MessageIndex <= Settings.MessageCount && Buffer->Pop(data)) {
                    MessageIndex++;
                }
                if (Buffer->IsFinished()) {
                    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS, "TEST FINISHED SelfId=" << SelfId() << ", ChannelId=" << ChannelId
                        << ", Role=" << (Role == TEvTestPrivate::ERole::Producer ? "Producer" : "Consumer"));
                    Send(RunnerId, new TEvTestPrivate::TEvFinished(Role, false));
                    PassAway();
                }
                break;
            }
        }
    }

    void HandleWakeup(NActors::TEvents::TEvWakeup::TPtr&) {
        Run();
    }

    void HandleResume(TEvDqCompute::TEvResumeExecution::TPtr&) {
        Run();
    }

    void HandleStart(TEvTestPrivate::TEvStart::TPtr& ev) {
        RunnerId = ev->Sender;
        PeerId = ev->Get()->PeerId;
        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS,
            "TEST WORKER START SelfId=" << SelfId() << ", ChannelId=" << ChannelId << ", PeerId=" << PeerId << ", Role=" << (Role == TEvTestPrivate::ERole::Producer ? "Producer" : "Consumer")
        );
        if (Settings.StartDelayMs) {
            Schedule(TDuration::MilliSeconds(RandomNumber<ui64>(Settings.StartDelayMs) + 1), new NActors::TEvents::TEvWakeup());
        } else {
            Run();
        }
    }

    void HandleAbort(NYql::NDq::TEvDq::TEvAbortExecution::TPtr& ev) {
        LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_CHANNELS,
            "TEST WORKER ABORT SelfId=" << SelfId() << ", ChannelId=" << ChannelId << ", " << ev->Get()->GetIssues().ToOneLineString()
        );
        Send(RunnerId, new TEvTestPrivate::TEvFinished(Role, true));
        PassAway();
    }

    std::shared_ptr<IDqChannelService> Service;
    std::shared_ptr<IChannelBuffer> Buffer;
    TEvTestPrivate::ERole Role;
    ui32 ChannelId;
    NActors::TActorId PeerId;
    NActors::TActorId RunnerId;
    TWorkerSettings Settings;
    int MessageIndex = 0;
    bool Started = false;
};

struct TLoadTest {

    virtual void Prepare() {
        settings.NodeCount = Local ? 1 : 2;
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_CHANNELS, NActors::NLog::EPriority::PRI_TRACE);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::EPriority::PRI_CRIT;
        if (Local) {
            NodeIndex1 = NodeIndex0;
        }
    }

    virtual void Init() {
        Runner = std::make_unique<TKikimrRunner>(settings);
        Runtime = Runner->GetTestServer().GetRuntime();
        Runtime->SetUseRealInterconnect();

        Control0 = Runtime->AllocateEdgeActor(0);
        Control1 = Local ? Control0 : Runtime->AllocateEdgeActor(1);

        Runtime->Send(MakeChannelServiceActorID(Runtime->GetNodeId(0)), Control0, new TEvPrivate::TEvServiceLookup(), NodeIndex0);
        auto serviceReply = Runtime->GrabEdgeEvent<TEvPrivate::TEvServiceReply>(Control0)->Release();
        Service0 = serviceReply->Service;

        if (Local) {
            Service1 = Service0;
        } else {
            Runtime->Send(MakeChannelServiceActorID(Runtime->GetNodeId(1)), Control1, new TEvPrivate::TEvServiceLookup(), NodeIndex1);
            auto serviceReply = Runtime->GrabEdgeEvent<TEvPrivate::TEvServiceReply>(Control1)->Release();
            Service1 = serviceReply->Service;
        }
    }

    virtual void Start() {
        for (auto i = 0; i < Count; i ++) {
            auto channelId = i + 1;
            if ((i & 1) == 0) {
                auto producer = Runtime->Register(new TWorkerActor(Service0, TEvTestPrivate::ERole::Producer, channelId, ProducerSettings), NodeIndex0);
                auto consumer = Runtime->Register(new TWorkerActor(Service1, TEvTestPrivate::ERole::Consumer, channelId, ConsumerSettings), NodeIndex1);
                Runtime->Send(consumer, Control1, new TEvTestPrivate::TEvStart(producer), NodeIndex1, true);
                Runtime->Send(producer, Control0, new TEvTestPrivate::TEvStart(consumer), NodeIndex0, true);
                Actors.insert(producer);
                Actors.insert(consumer);
            } else {
                auto producer = Runtime->Register(new TWorkerActor(Service1, TEvTestPrivate::ERole::Producer, channelId, ProducerSettings), NodeIndex1);
                auto consumer = Runtime->Register(new TWorkerActor(Service0, TEvTestPrivate::ERole::Consumer, channelId, ConsumerSettings), NodeIndex0);
                Runtime->Send(consumer, Control0, new TEvTestPrivate::TEvStart(producer), NodeIndex0, true);
                Runtime->Send(producer, Control1, new TEvTestPrivate::TEvStart(consumer), NodeIndex1, true);
                Actors.insert(producer);
                Actors.insert(consumer);
            }
        }
    }

    virtual void Wait() {
        try {
            for (auto i = 0; i < Count; i++) {
                auto msg0 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control0, TDuration::Seconds(10));
                Actors.erase(msg0->Sender);
                FinishCount[NodeIndex0][msg0->Get()->Role]++;
                ErrorCount += msg0->Get()->Error;
                auto msg1 = Runtime->GrabEdgeEvent<TEvTestPrivate::TEvFinished>(Control1, TDuration::Seconds(10));
                Actors.erase(msg1->Sender);
                FinishCount[NodeIndex1][msg1->Get()->Role]++;
                ErrorCount += msg1->Get()->Error;
            }
        } catch (NActors::TEmptyEventQueueException&) {
            if (!Actors.empty()) {
                TStringBuilder builder;
                builder << "NOT FINISHED ACTORS ";
                for (auto actorId : Actors) {
                    builder << ' ' << actorId;
                }
                UNIT_ASSERT_C(false, builder);
            }
        }
    }

    virtual void Check() {
        if (Local) {
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[0][TEvTestPrivate::ERole::Producer], Count);
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[0][TEvTestPrivate::ERole::Consumer], Count);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[0][TEvTestPrivate::ERole::Producer], (Count + 1) / 2);
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[0][TEvTestPrivate::ERole::Consumer], Count / 2);
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[1][TEvTestPrivate::ERole::Producer], Count / 2);
            UNIT_ASSERT_VALUES_EQUAL(FinishCount[1][TEvTestPrivate::ERole::Consumer], (Count + 1) / 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(ErrorCount, 0);
    }

    virtual void Run() {
        Prepare();
        Init();
        Start();
        Wait();
        Check();
    }

    int Count = 1;
    bool Local = true;
    ui32 NodeIndex0 = 0;
    ui32 NodeIndex1 = 1;
    TKikimrSettings settings;
    std::unique_ptr<TKikimrRunner> Runner;
    NActors::TTestActorRuntime* Runtime;
    std::shared_ptr<TDqChannelService> Service0;
    std::shared_ptr<TDqChannelService> Service1;
    NActors::TActorId Control0;
    NActors::TActorId Control1;
    TWorkerSettings ProducerSettings;
    TWorkerSettings ConsumerSettings;
    THashSet<NActors::TActorId> Actors;
    int ErrorCount = 0;
    int FinishCount[2][2] = {{0, 0}, {0, 0}};
};

Y_UNIT_TEST_SUITE(Channels20) {

    void LoadTest(int count, bool local, const TWorkerSettings& producerSettings, const TWorkerSettings& consumerSettings, const TFailureSettings& = TFailureSettings{}) {
        TLoadTest test;

        test.Count = count;
        test.Local = local;
        test.ProducerSettings = producerSettings;
        test.ConsumerSettings = consumerSettings;

        test.Run();
    }

    void LoadTest(int count, bool local, const TWorkerSettings& settings = TWorkerSettings{}, const TFailureSettings& failureSettings = TFailureSettings{}) {
        LoadTest(count, local, settings, settings, failureSettings);
    }

    Y_UNIT_TEST(EmptyFinish2n) {
        LoadTest(100, false);
    }

    Y_UNIT_TEST(SimpleFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100 });
    }

    Y_UNIT_TEST(EarlyFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 50, .EarlyFinish = true });
    }

    Y_UNIT_TEST(InstantFinish2n) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 10 }, TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true });
    }

    Y_UNIT_TEST(EmptyFinish1n) {
        LoadTest(100, true);
    }

    Y_UNIT_TEST(SimpleFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 100 });
    }

    Y_UNIT_TEST(EarlyFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 50, .EarlyFinish = true });
    }

    Y_UNIT_TEST(InstantFinish1n) {
        LoadTest(100, true, TWorkerSettings{ .MessageCount = 10 }, TWorkerSettings{ .MessageCount = 0, .EarlyFinish = true });
    }

    Y_UNIT_TEST(MissedData) {
        LoadTest(100, false, TWorkerSettings{ .MessageCount = 100 }, TWorkerSettings{ .MessageCount = 100 }, TFailureSettings{ .Data = 10 });
    }
}
