#include "dq_channel_service.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/dq/runtime/dq_channel_service_impl.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <library/cpp/threading/mux_event/mux_event.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

using namespace NKikimr::NKqp;
using namespace NYql::NDq;

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(Channels20) {

    bool ReadBlocked(std::shared_ptr<IChannelBuffer>& buffer, TDataChunk& data, TDuration timeout = TDuration::Seconds(10)) {
        auto deadline = TInstant::Now() + timeout;
        while (true) {
            if (buffer->Pop(data)) {
                return true;
            }
            Sleep(TDuration::MilliSeconds(10));
            if (TInstant::Now() > deadline) {
                return false;
            }
        }
    }

    bool ReadAsyncNotified(NActors::TTestActorRuntime& runtime, NActors::TActorId receiver, std::shared_ptr<IChannelBuffer>& buffer, TDataChunk& data, TDuration timeout = TDuration::Seconds(10)) {
        if (runtime.RunCall([&buffer, &data] { return buffer->Pop(data); })) {
            return true;
        }

        auto resume = runtime.GrabEdgeEvent<TEvDqCompute::TEvResumeExecution>(receiver, timeout)->Release();

        return resume && runtime.RunCall([&buffer, &data] { return buffer->Pop(data); });
    }

    Y_UNIT_TEST(LocalChannelBackPressure) {

        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetLocalChannelInflightBytes(512);
        TKikimrRunner kikimr(settings);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto receiver = runtime.AllocateEdgeActor();

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId()), sender, new TEvPrivate::TEvServiceLookup());
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
        auto service = serviceReply->Service;

        TChannelFullInfo info(1, sender, receiver, 1, 2, TCollectStatsLevel::None);
        auto senderBuffer = service->GetLocalBuffer(info, false, nullptr);
        auto receiverBuffer = service->GetLocalBuffer(info, true, nullptr);

        auto aggregator = std::make_shared<TDqFillAggregator>();
        senderBuffer->SetFillAggregator(aggregator);
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        UNIT_ASSERT(receiverBuffer->IsEmpty());

        while (senderBuffer->GetFillLevel() == EDqFillLevel::NoLimit) {
            runtime.RunCall([&senderBuffer] {
                senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, senderBuffer->GetLeading(), false));
                return true;
            });
        }
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::HardLimit);

        ui64 totalCount = 0;
        ui64 deltaCount = 0;

        while (true) {
            TDataChunk data;
            if (!runtime.RunCall([&receiverBuffer, &data] { return receiverBuffer->Pop(data); })) {
                break;
            }
            totalCount++;
            if (aggregator->GetFillLevel() == EDqFillLevel::HardLimit) {
                deltaCount++;
            }
            UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
        }
        UNIT_ASSERT(receiverBuffer->IsEmpty());
        // check for hysteresis
        UNIT_ASSERT(deltaCount * 4 < totalCount);
        UNIT_ASSERT(deltaCount * 6 > totalCount);
    }
/*
    Y_UNIT_TEST(LocalChannelConcurrency) {
        constexpr ui32 MESSAGE_COUNT = 10;

        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetLocalChannelInflightBytes(128);
        TKikimrRunner kikimr(settings);

        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto receiver = runtime.AllocateEdgeActor();

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId()), sender, new TEvPrivate::TEvServiceLookup());
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
        auto service = serviceReply->Service;

        TChannelInfo info(1, sender, receiver);
        auto senderBuffer = service->GetLocalBuffer(info, false);
        auto receiverBuffer = service->GetLocalBuffer(info, true);

        TMuxEvent event;
        ui32 receivedCount = 0;
        ui32 outputBlockCount = 0;
        ui32 inputBlockCount = 0;
        NPar::LocalExecutor().RunAdditionalThreads(2);
        NPar::LocalExecutor().Exec([&](int) mutable {
            bool blocked = false;
            for (ui32 i = 0; i < MESSAGE_COUNT; i++) {
                // Sleep(TDuration::MicroSeconds(i / (MESSAGE_COUNT / 10)));
                while (senderBuffer->GetFillLevel() == EDqFillLevel::HardLimit) {
                    if (!blocked) {
                        blocked = true;
                        outputBlockCount++;
                    }
                    // Sleep(TDuration::MicroSeconds(i / (MESSAGE_COUNT / 100)));
                }
                blocked = false;
                runtime.RunCall([&senderBuffer] {
                    senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, senderBuffer->GetLeading(), false));
                    return true;
                });
            }
            runtime.RunCall([&senderBuffer] {
                senderBuffer->Push(TDataChunk(false, true));
                return true;
            });
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);
        NPar::LocalExecutor().Exec([&](int) mutable {
            bool blocked = false;
            while (true) {
                Sleep(TDuration::MicroSeconds(10 - receivedCount / (MESSAGE_COUNT / 10)));
                if (receiverBuffer->IsEmpty()) {
                    if (!blocked) {
                        blocked = true;
                        inputBlockCount++;
                    }
                    continue;
                }
                TDataChunk data;
                UNIT_ASSERT(runtime.RunCall([&receiverBuffer, &data] { return receiverBuffer->Pop(data); } ));
                if (data.Finished) {
                    break;
                }
                receivedCount++;
                blocked = false;
                UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
            }
            event.Signal();
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);
        event.WaitI();
        UNIT_ASSERT_VALUES_EQUAL(receivedCount, MESSAGE_COUNT);
        Cerr << outputBlockCount << " / " << inputBlockCount << Endl;
    }

    Y_UNIT_TEST(LocalChannelEarlyFinish) {
        constexpr ui32 MESSAGE_COUNT = 1000;

        TKikimrRunner kikimr(TKikimrSettings{});
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto receiver = runtime.AllocateEdgeActor();

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId()), sender, new TEvPrivate::TEvServiceLookup());
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
        auto service = serviceReply->Service;

        TChannelFullInfo info(1, sender, receiver, 1, 2, TCollectStatsLevel::None);
        auto senderBuffer = service->GetLocalBuffer(info, false, nullptr);
        auto receiverBuffer = service->GetLocalBuffer(info, true, nullptr);

        TMuxEvent eventR;
        TMuxEvent eventS;
        ui32 sentCount = 0;
        ui32 receivedCount = 0;
        NPar::LocalExecutor().RunAdditionalThreads(2);
        NPar::LocalExecutor().Exec([&](int) mutable {
            for (ui32 i = 0; i < MESSAGE_COUNT; i++) {
                sentCount++;
                runtime.RunCall([&senderBuffer] {
                    senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, senderBuffer->GetLeading(), false));
                    return true;
                });
                Sleep(TDuration::MicroSeconds(1));
                if (senderBuffer->IsEarlyFinished()) {
                    break;
                }
            }
            eventS.Signal();
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);
        NPar::LocalExecutor().Exec([&](int) mutable {
            while (true) {
                if (receiverBuffer->IsEmpty()) {
                    Sleep(TDuration::MicroSeconds(10));
                    continue;
                }
                TDataChunk data;
                UNIT_ASSERT(runtime.RunCall([&receiverBuffer, &data] { return receiverBuffer->Pop(data); }));
                if (data.Finished) {
                    break;
                }
                receivedCount++;
                UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
                if (receivedCount > MESSAGE_COUNT / 4) {
                    receiverBuffer->EarlyFinish();
                    break;
                }
            }
            eventR.Signal();
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);
        eventS.WaitI();
        eventR.WaitI();
        UNIT_ASSERT(sentCount < MESSAGE_COUNT);
        Cerr << sentCount << " / " << receivedCount << Endl;
    }
*/
    Y_UNIT_TEST(LocalChannelAsyncRead) {

        TKikimrRunner kikimr(TKikimrSettings{});
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        auto receiver = runtime.AllocateEdgeActor();

        runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId()), sender, new TEvPrivate::TEvServiceLookup());
        auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
        auto service = serviceReply->Service;

        TChannelFullInfo info(1, sender, receiver, 1, 2, TCollectStatsLevel::None);
        auto senderBuffer = service->GetLocalBuffer(info, false, nullptr);
        auto receiverBuffer = service->GetLocalBuffer(info, true, nullptr);

        UNIT_ASSERT(receiverBuffer->IsEmpty());

        runtime.RunCall([&senderBuffer] {
            senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, senderBuffer->GetLeading(), false));
            return true;
        });

        TDataChunk data;
        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
    }

    Y_UNIT_TEST(IcChannelTrivial) {

        TVector<NKikimrKqp::TKqpSetting> kqpSettings;
        TKikimrRunner kikimr(kqpSettings, "", KikimrDefaultUtDomainRoot, 2);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelFullInfo info(1, sender, receiver, 1, 2, TCollectStatsLevel::None);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderBuffer = service->GetRemoteOutputBuffer(info, nullptr);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, true, false));
        senderBuffer->Push(TDataChunk(false, true));

        TDataChunk data;

        UNIT_ASSERT(ReadBlocked(receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
        UNIT_ASSERT(ReadBlocked(receiverBuffer, data));
        UNIT_ASSERT(data.Finished);
    }

    Y_UNIT_TEST(IcChannelLateBinding) {

        TVector<NKikimrKqp::TKqpSetting> kqpSettings;
        TKikimrRunner kikimr(kqpSettings, "", KikimrDefaultUtDomainRoot, 2);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<TDqChannelService> service0;
        std::shared_ptr<TDqChannelService> service1;

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            service0 = serviceReply->Service;
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            service1 = serviceReply->Service;
        }

        TChannelFullInfo info1(1, sender, receiver, 1, 2, TCollectStatsLevel::None);
        TChannelFullInfo info2(2, sender, receiver, 3, 4, TCollectStatsLevel::None);

        std::shared_ptr<IChannelBuffer> senderBuffer1 = service0->GetRemoteOutputBuffer(info1, nullptr);
        std::shared_ptr<IChannelBuffer> senderBuffer2 = service0->GetRemoteOutputBuffer(info2, nullptr);
        std::shared_ptr<IChannelBuffer> receiverBuffer1 = service1->GetRemoteInputBuffer(info1);

        senderBuffer2->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, true, false));
        senderBuffer1->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, true, false));
        senderBuffer2->Push(TDataChunk(false, true));
        senderBuffer1->Push(TDataChunk(false, true));

        TDataChunk data;

        UNIT_ASSERT(ReadBlocked(receiverBuffer1, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
        UNIT_ASSERT(ReadBlocked(receiverBuffer1, data));
        UNIT_ASSERT(data.Finished);

        std::shared_ptr<IChannelBuffer> receiverBuffer2 = service1->GetRemoteInputBuffer(info2);

        UNIT_ASSERT(ReadBlocked(receiverBuffer2, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
        UNIT_ASSERT(ReadBlocked(receiverBuffer2, data));
        UNIT_ASSERT(data.Finished);
    }

    Y_UNIT_TEST(IcChannelAsyncRead) {

        TVector<NKikimrKqp::TKqpSetting> kqpSettings;
        TKikimrRunner kikimr(kqpSettings, "", KikimrDefaultUtDomainRoot, 2);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelFullInfo info(1, sender, receiver, 1, 2, TCollectStatsLevel::None);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderBuffer = service->GetRemoteOutputBuffer(info, nullptr);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        UNIT_ASSERT(receiverBuffer->IsEmpty());
        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, true, true));

        TDataChunk data;
        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");
    }

    Y_UNIT_TEST(IcChannelEarlyFinish) {

        TVector<NKikimrKqp::TKqpSetting> kqpSettings;
        TKikimrRunner kikimr(kqpSettings, "", KikimrDefaultUtDomainRoot, 2);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelFullInfo info(1, sender, receiver, 1, 2, TCollectStatsLevel::None);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderBuffer = service->GetRemoteOutputBuffer(info, nullptr);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, true, false));

        TDataChunk data;
        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");

        receiverBuffer->EarlyFinish();
        auto deadline = TInstant::Now() + TDuration::Seconds(10);
        bool earlyFinished = false;
        while (TInstant::Now() <= deadline) {
            if (senderBuffer->IsFinished()) {
                earlyFinished = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(earlyFinished);
    }

    Y_UNIT_TEST(IcChannelBackPressure) {

        TKikimrSettings settings;
        settings.SetNodeCount(2);
        settings.AppConfig.MutableTableServiceConfig()->SetRemoteChannelInflightBytes(10);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelFullInfo info(1, sender, receiver, 1, 2, TCollectStatsLevel::None);

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderBuffer = service->GetRemoteOutputBuffer(info, nullptr);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, true, false));

        auto aggregator = std::make_shared<TDqFillAggregator>();
        senderBuffer->SetFillAggregator(aggregator);
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("ByeBye"), 1, false, true));
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::HardLimit);

        TDataChunk data;

        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");

        auto deadline = TInstant::Now() + TDuration::Seconds(10);
        bool noLimit = false;
        while (TInstant::Now() <= deadline) {
            if (aggregator->GetFillLevel() == EDqFillLevel::NoLimit) {
                noLimit = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(10));
        }
        UNIT_ASSERT(noLimit);

        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "ByeBye");
    }
/*
    Y_UNIT_TEST(IcChannelSessionInflight) {

        TKikimrSettings settings;
        settings.SetNodeCount(2);
        settings.AppConfig.MutableTableServiceConfig()->SetNodeSessionIcInflightBytes(8);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        auto sender = runtime.AllocateEdgeActor(0);
        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<IChannelBuffer> senderBuffer;
        std::shared_ptr<TDebugNodeState> senderState;
        std::shared_ptr<IChannelBuffer> receiverBuffer;
        TChannelFullInfo info(1, sender, receiver, 1, 2, TCollectStatsLevel::None);
        std::shared_ptr<TDqChannelService> service0;
        std::shared_ptr<TDqChannelService> service1;

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), sender, new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(sender)->Release();
            auto service = serviceReply->Service;
            senderState = service->CreateDebugNodeState(runtime.GetNodeId(1));
            senderBuffer = service->GetRemoteOutputBuffer(info, nullptr);
        }
        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            receiverBuffer = service->GetRemoteInputBuffer(info);
        }

        auto aggregator = std::make_shared<TDqFillAggregator>();
        senderBuffer->SetFillAggregator(aggregator);

        senderState->PauseChannelAck();

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Hello"), 1, true, false));
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("ByeBye"), 1, false, false));
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("IamBack"), 1, false, true));
        UNIT_ASSERT_VALUES_EQUAL(aggregator->GetFillLevel(), EDqFillLevel::NoLimit);

        TDataChunk data;

        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "Hello");

        UNIT_ASSERT(ReadAsyncNotified(runtime, receiver, receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "ByeBye");

        UNIT_ASSERT(!ReadBlocked(receiverBuffer, data));

        senderState->ResumeChannelAck();

        UNIT_ASSERT(ReadBlocked(receiverBuffer, data));
        UNIT_ASSERT(data.Buffer.Front().Buf == "IamBack");
    }

    Y_UNIT_TEST(IcChannelSessionWaiters) {

        constexpr ui32 SENDER_COUNT = 10;
        constexpr ui32 MESSAGE_COUNT = 1000;

        TKikimrSettings settings;
        settings.SetNodeCount(2);
        settings.AppConfig.MutableTableServiceConfig()->SetRemoteChannelInflightBytes(400);
        settings.AppConfig.MutableTableServiceConfig()->SetNodeSessionIcInflightBytes(4);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();

        runtime.SetLogPriority(NKikimrServices::KQP_CHANNELS, NActors::NLog::PRI_ERROR);

        NActors::TActorId senders[SENDER_COUNT];

        for (ui32 i = 0; i < SENDER_COUNT; i++) {
            senders[i] = runtime.AllocateEdgeActor(0);
        }

        auto receiver = runtime.AllocateEdgeActor(1);

        std::shared_ptr<TOutputBuffer> senderBuffers[SENDER_COUNT];
        std::shared_ptr<TInputBuffer> receiverBuffers[SENDER_COUNT];

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(1)), receiver, new TEvPrivate::TEvServiceLookup(), 1);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(receiver)->Release();
            auto service = serviceReply->Service;
            for (ui32 i = 0; i < SENDER_COUNT; i++) {
                receiverBuffers[i] = service->GetRemoteInputBuffer(TChannelFullInfo(i + 1, senders[i], receiver, 1, 2, TCollectStatsLevel::None));
            }
        }

        {
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(0)), senders[0], new TEvPrivate::TEvServiceLookup(), 0);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(senders[0])->Release();
            auto service = serviceReply->Service;
            for (ui32 i = 0; i < SENDER_COUNT; i++) {
                senderBuffers[i] = service->GetRemoteOutputBuffer(TChannelFullInfo(i + 1, senders[i], receiver, 1, 2, TCollectStatsLevel::None), nullptr);
            }
        }

        for (ui32 i = 0; i < SENDER_COUNT; i++) {
            senderBuffers[i]->SetFillAggregator(std::make_shared<TDqFillAggregator>());
        }

        TMuxEvent events[SENDER_COUNT];
        TMuxEvent event;

        auto sendLambda = [](std::shared_ptr<TOutputBuffer>& senderBuffer, TMuxEvent& event) {

            for (ui32 i = 0; i < MESSAGE_COUNT; i++) {
                while (senderBuffer->Descriptor->Aggregator->GetFillLevel() != EDqFillLevel::NoLimit) {
                    // Sleep(TDuration::MicroSeconds(1));
                }
                senderBuffer->Push(TDataChunk(NYql::TChunkedBuffer("Piing"), 1, i == 0, false));
            }
            senderBuffer->SendFinish();
            Cerr << TStringBuilder() << "Writer finished " << senderBuffer->Descriptor->PushBytes.load() << " bytes" << Endl;
            event.Signal();
        };

        NPar::LocalExecutor().RunAdditionalThreads(SENDER_COUNT + 1);
        NPar::LocalExecutor().Exec([&](int) mutable {
            ui32 finished = 0;
            while (true) {
                auto received = false;
                TDataChunk data;

                for (ui32 i = 0; i < SENDER_COUNT; i++) {
                    if (runtime.RunCall([&receiverBuffers, &data, i=i] { return receiverBuffers[i]->Pop(data); })) {
                        received = true;
                        if (data.Finished) {
                            finished++;
                        }
                    }
                }

                if (finished == 4) {
                    break;
                }

                if (!received) {
                    // Sleep(TDuration::MicroSeconds(2));
                }
            }
            Cerr << TStringBuilder() << "Reader finished {";
            for (ui32 i = 0; i < SENDER_COUNT; i++) {
                if (i) {
                    Cerr << ", ";
                }
                Cerr << receiverBuffers[i]->Descriptor->PopBytes.load();
            }
            Cerr << "} bytes" << Endl;
            event.Signal();
        }, 0, NPar::TLocalExecutor::MED_PRIORITY);

        for (ui32 i = 0; i < SENDER_COUNT; i++) {
            NPar::LocalExecutor().Exec([&](int index) mutable { sendLambda(senderBuffers[index], events[index]); }, i, NPar::TLocalExecutor::MED_PRIORITY);
        }

        for (ui32 i = 0; i < SENDER_COUNT; i++) {
            events[i].WaitI();
        }
        event.WaitI();
    }
*/
    Y_UNIT_TEST(CaIntegrationTrivial) {

        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetDqChannelVersion(1);
        TKikimrRunner kikimr(settings);

        auto client = kikimr.GetQueryClient();

        auto result = client.ExecuteQuery(R"(
            PRAGMA ydb.DqChannelVersion = "2";
            SELECT 1;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();

        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[1]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(CaIntegrationAgg) {

        TKikimrSettings settings;
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        settings.AppConfig.MutableTableServiceConfig()->SetDqChannelVersion(1);
        TKikimrRunner kikimr(settings);

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        {
            const TString query = R"(
                CREATE TABLE `/Root/Table1` (
                    k1 Uint32 NOT NULL,
                    PRIMARY KEY (k1)
                )
                PARTITION BY HASH (k1)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Table1` (k1) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString query = R"(
                PRAGMA ydb.DqChannelVersion = "2";
                SELECT SUM(k1) FROM `/Root/Table1`
            )";

            NYdb::NTable::TExecDataQuerySettings settings;
            settings.CollectQueryStats(ECollectQueryStatsMode::Full);
            auto result = session.ExecuteDataQuery(query, TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), settings).GetValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(FormatResultSetYson(result.GetResultSet(0)), R"([[[55u]]])");
        }
    }

    Y_UNIT_TEST(CaIntegrationIc) {

        TKikimrSettings settings;
        settings.SetNodeCount(4);
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        settings.AppConfig.MutableTableServiceConfig()->SetDqChannelVersion(1);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_ERROR);

        std::shared_ptr<TDqChannelService> services[4];
        for (auto i = 0u; i < 4; i++) {
            auto edgeActor = runtime.AllocateEdgeActor(i);
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(i)), edgeActor, new TEvPrivate::TEvServiceLookup(), i);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(edgeActor)->Release();
            services[i] = serviceReply->Service;
        }

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        {
            const TString query = R"(
                CREATE TABLE `/Root/Table1` (
                    k1 Uint32 NOT NULL,
                    PRIMARY KEY (k1)
                )
                PARTITION BY HASH (k1)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Table1` (k1) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();

        auto future = client.ExecuteQuery(R"(
            PRAGMA ydb.DqChannelVersion = "2";
            SELECT SUM(k1) FROM `/Root/Table1`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx());

        while (!future.HasValue()) {
            // for (auto i = 0u; i < 4; i++) {
            //     Cerr << Endl << services[i]->GetDebugInfo() << Endl;
            // }
            Sleep(TDuration::Seconds(1));
        }

        auto result = future.GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[[55u]]]", FormatResultSetYson(result.GetResultSet(0)));
    }
/*
    Y_UNIT_TEST(CaIntegrationDataLoss) {

        TKikimrSettings settings;
        settings.SetNodeCount(4);
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableFastChannels(false);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_ERROR);
        runtime.SetLogPriority(NKikimrServices::KQP_CHANNELS, NActors::NLog::PRI_DEBUG);

        std::shared_ptr<TDqChannelService> services[4];
        for (auto i = 0u; i < 4; i++) {
            auto edgeActor = runtime.AllocateEdgeActor(i);
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(i)), edgeActor, new TEvPrivate::TEvServiceLookup(), i);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(edgeActor)->Release();
            services[i] = serviceReply->Service;
        }

        for (auto i = 0u; i < 4; i++) {
            for (auto j = 0u; j < 4; j++) {
                if (i != j) {
                    services[i]->CreateDebugNodeState(runtime.GetNodeId(j))->SetLossProbability(0.1, 100, 0, 0);
                }
            }
        }

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        {
            const TString query = R"(
                CREATE TABLE `/Root/Table1` (
                    k1 Uint32 NOT NULL,
                    PRIMARY KEY (k1)
                )
                PARTITION BY HASH (k1)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Table1` (k1) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();

        auto future = client.ExecuteQuery(R"(
            PRAGMA ydb.UseFastChannels = "true";
            SELECT SUM(k1) FROM `/Root/Table1`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx());

        while (!future.HasValue()) {
            // for (auto i = 0u; i < 4; i++) {
            //     Cerr << Endl << services[i]->GetDebugInfo() << Endl;
            // }
            Sleep(TDuration::Seconds(1));
        }

        auto result = future.GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[[55u]]]", FormatResultSetYson(result.GetResultSet(0)));
    }

    Y_UNIT_TEST(CaIntegrationAckLoss) {

        TKikimrSettings settings;
        settings.SetNodeCount(4);
        settings.AppConfig.MutableTableServiceConfig()->SetAllowOlapDataQuery(true);
        settings.AppConfig.MutableTableServiceConfig()->SetEnableFastChannels(false);

        TKikimrRunner kikimr(settings);
        auto& runtime = *kikimr.GetTestServer().GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_ERROR);

        std::shared_ptr<TDqChannelService> services[4];
        for (auto i = 0u; i < 4; i++) {
            auto edgeActor = runtime.AllocateEdgeActor(i);
            runtime.Send(MakeChannelServiceActorID(runtime.GetNodeId(i)), edgeActor, new TEvPrivate::TEvServiceLookup(), i);
            auto serviceReply = runtime.GrabEdgeEvent<TEvPrivate::TEvServiceReply>(edgeActor)->Release();
            services[i] = serviceReply->Service;
        }

        for (auto i = 0u; i < 4; i++) {
            for (auto j = 0u; j < 4; j++) {
                if (i != j) {
                    services[i]->CreateDebugNodeState(runtime.GetNodeId(j))->SetLossProbability(0.0, 0, 0.1, 100);
                }
            }
        }

        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        {
            const TString query = R"(
                CREATE TABLE `/Root/Table1` (
                    k1 Uint32 NOT NULL,
                    PRIMARY KEY (k1)
                )
                PARTITION BY HASH (k1)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4);
            )";
            auto result = session.ExecuteSchemeQuery(query).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.ExecuteDataQuery(R"(
            UPSERT INTO `/Root/Table1` (k1) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
            )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        auto client = kikimr.GetQueryClient();

        auto future = client.ExecuteQuery(R"(
            PRAGMA ydb.UseFastChannels = "true";
            SELECT SUM(k1) FROM `/Root/Table1`;
        )", NYdb::NQuery::TTxControl::BeginTx().CommitTx());

        while (!future.HasValue()) {
            // for (auto i = 0u; i < 4; i++) {
            //     Cerr << Endl << services[i]->GetDebugInfo() << Endl;
            // }
            Sleep(TDuration::Seconds(1));
        }

        auto result = future.GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        CompareYson("[[[55u]]]", FormatResultSetYson(result.GetResultSet(0)));
    }
*/
}