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

Y_UNIT_TEST_SUITE(Channels20) {

    void LoadTest(int count, bool local, const TWorkerSettings& producerSettings, const TWorkerSettings& consumerSettings) {

        TKikimrSettings settings;
        settings.NodeCount = local ? 1 : 2;
        settings.LogSettings = TTestLogSettings().AddLogPriority(NKikimrServices::KQP_CHANNELS, NActors::NLog::EPriority::PRI_TRACE);
        settings.LogSettings->DefaultLogPriority = NActors::NLog::EPriority::PRI_CRIT;
        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto control0 = runtime.AllocateEdgeActor(0);
        auto control1 = local ? control0 : runtime.AllocateEdgeActor(1);

        std::shared_ptr<TDqChannelService> service0;
        std::shared_ptr<TDqChannelService> service1;

        ui32 nodeIndex0 = 0;
        ui32 nodeIndex1 = local ? 0 : 1;

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), control0, new TEvPrivate::TEvServiceLookup(), nodeIndex0);
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(control0)->Release();
        service0 = serviceReply->Service;

        if (local) {
            service1 = service0;
        } else {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), control1, new TEvPrivate::TEvServiceLookup(), nodeIndex1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(control1)->Release();
            service1 = serviceReply->Service;
        }

        THashSet<NActors::TActorId> actors;

        for (auto i = 0; i < count; i ++) {
            auto channelId = i + 1;
            if ((i & 1) == 0) {
                auto producer = runtime.Register(new TWorkerActor(service0, TEvTestPrivate::ERole::Producer, channelId, producerSettings), nodeIndex0);
                auto consumer = runtime.Register(new TWorkerActor(service1, TEvTestPrivate::ERole::Consumer, channelId, consumerSettings), nodeIndex1);
                runtime.Send(consumer, control1, new TEvTestPrivate::TEvStart(producer), nodeIndex1, true);
                runtime.Send(producer, control0, new TEvTestPrivate::TEvStart(consumer), nodeIndex0, true);
                actors.insert(producer);
                actors.insert(consumer);
            } else {
                auto producer = runtime.Register(new TWorkerActor(service1, TEvTestPrivate::ERole::Producer, channelId, producerSettings), nodeIndex1);
                auto consumer = runtime.Register(new TWorkerActor(service0, TEvTestPrivate::ERole::Consumer, channelId, consumerSettings), nodeIndex0);
                runtime.Send(consumer, control0, new TEvTestPrivate::TEvStart(producer), nodeIndex0, true);
                runtime.Send(producer, control1, new TEvTestPrivate::TEvStart(consumer), nodeIndex1, true);
                actors.insert(producer);
                actors.insert(consumer);
            }
        }

        int finishCount[2][2] = {{0, 0}, {0, 0}};
        int errorCount = 0;
        try {
            for (auto i = 0; i < count; i++) {
                auto msg0 = runtime.GrabEdgeEvent<TEvTestPrivate::TEvFinished>(control0, TDuration::Seconds(10));
                actors.erase(msg0->Sender);
                finishCount[nodeIndex0][msg0->Get()->Role]++;
                errorCount += msg0->Get()->Error;
                auto msg1 = runtime.GrabEdgeEvent<TEvTestPrivate::TEvFinished>(control1, TDuration::Seconds(10));
                actors.erase(msg1->Sender);
                finishCount[nodeIndex1][msg1->Get()->Role]++;
                errorCount += msg1->Get()->Error;
            }
        } catch (NActors::TEmptyEventQueueException&) {
            if (!actors.empty()) {
                TStringBuilder builder;
                builder << "NOT FINISHED ACTORS ";
                for (auto actorId : actors) {
                    builder << ' ' << actorId;
                }
                UNIT_ASSERT_C(false, builder);
            }
        }

        if (local) {
            UNIT_ASSERT_VALUES_EQUAL(finishCount[0][TEvTestPrivate::ERole::Producer], count);
            UNIT_ASSERT_VALUES_EQUAL(finishCount[0][TEvTestPrivate::ERole::Consumer], count);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(finishCount[0][TEvTestPrivate::ERole::Producer], (count + 1) / 2);
            UNIT_ASSERT_VALUES_EQUAL(finishCount[0][TEvTestPrivate::ERole::Consumer], count / 2);
            UNIT_ASSERT_VALUES_EQUAL(finishCount[1][TEvTestPrivate::ERole::Producer], count / 2);
            UNIT_ASSERT_VALUES_EQUAL(finishCount[1][TEvTestPrivate::ERole::Consumer], (count + 1) / 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(errorCount, 0);
    }

    void LoadTest(int count, bool local, const TWorkerSettings& settings = TWorkerSettings{}) {
        LoadTest(count, local, settings, settings);
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
}
