#include "multicluster_consumer.h"
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>

#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/event.h>

namespace NPersQueue {

Y_UNIT_TEST_SUITE(TMultiClusterConsumerTest) {
    TConsumerSettings MakeFakeConsumerSettings() {
        TConsumerSettings settings;
        settings.Topics.push_back("topic");
        settings.ReadFromAllClusterSources = true;
        settings.ReadMirroredPartitions = false;
        settings.MaxAttempts = 1; // Consumer should fix this setting. Check it.
        return settings;
    }

    TConsumerSettings MakeConsumerSettings(const SDKTestSetup& setup) {
        TConsumerSettings settings = setup.GetConsumerSettings();
        settings.ReadFromAllClusterSources = true;
        settings.ReadMirroredPartitions = false;
        settings.MaxAttempts = 1; // Consumer should fix this setting. Check it.
        return settings;
    }

    Y_UNIT_TEST(NotStartedConsumerCanBeDestructed) {
        // Test that consumer doesn't hang on till shutdown
        TPQLib lib;
        lib.CreateConsumer(MakeFakeConsumerSettings(), new TCerrLogger(TLOG_DEBUG));
    }

    Y_UNIT_TEST(StartedConsumerCanBeDestructed) {
        // Test that consumer doesn't hang on till shutdown
        TPQLib lib;
        auto consumer = lib.CreateConsumer(MakeFakeConsumerSettings(), new TCerrLogger(TLOG_DEBUG));
        auto isDead = consumer->IsDead();
        consumer->Start();
        consumer = nullptr;
        isDead.GetValueSync();
    }

    Y_UNIT_TEST(CommitRightAfterStart) {
        SDKTestSetup setup("InfiniteStart");
        TPQLib lib;
        auto settings = MakeConsumerSettings(setup);
        settings.Topics[0] = "unknown_topic";
        auto consumer = lib.CreateConsumer(settings, new TCerrLogger(TLOG_DEBUG));
        consumer->Start();
        auto isDead = consumer->IsDead();
        consumer->Commit({42});
        if (GrpcV1EnabledByDefault()) {
            UNIT_ASSERT_EQUAL_C(isDead.GetValueSync().GetCode(), NErrorCode::WRONG_COOKIE, isDead.GetValueSync());
        } else {
            UNIT_ASSERT_STRINGS_EQUAL_C(isDead.GetValueSync().GetDescription(), "Requesting commit, but consumer is not in working state", isDead.GetValueSync());
        }
    }

    Y_UNIT_TEST(GetNextMessageRightAfterStart) {
        if (GrpcV1EnabledByDefault()) {
            return;
        }
        SDKTestSetup setup("InfiniteStart");
        TPQLib lib;
        auto settings = MakeConsumerSettings(setup);
        settings.Topics[0] = "unknown_topic";
        auto consumer = lib.CreateConsumer(settings, new TCerrLogger(TLOG_DEBUG));
        consumer->Start();
        auto nextMessage = consumer->GetNextMessage();
        UNIT_ASSERT_STRINGS_EQUAL(nextMessage.GetValueSync().Response.GetError().GetDescription(), "Requesting next message, but consumer is not in working state");
    }

    Y_UNIT_TEST(RequestPartitionStatusRightAfterStart) {
        if (GrpcV1EnabledByDefault()) {
            return;
        }
        SDKTestSetup setup("InfiniteStart");
        TPQLib lib;
        auto settings = MakeConsumerSettings(setup);
        settings.Topics[0] = "unknown_topic";
        auto consumer = lib.CreateConsumer(settings, new TCerrLogger(TLOG_DEBUG));
        consumer->Start();
        auto isDead = consumer->IsDead();
        consumer->RequestPartitionStatus("topic", 42, 42);
        UNIT_ASSERT_STRINGS_EQUAL(isDead.GetValueSync().GetDescription(), "Requesting partition status, but consumer is not in working state");
    }

    Y_UNIT_TEST(StartedConsumerCantBeStartedAgain) {
        TPQLib lib;
        auto consumer = lib.CreateConsumer(MakeFakeConsumerSettings(), new TCerrLogger(TLOG_DEBUG));
        auto startFuture = consumer->Start();
        auto startFuture2 = consumer->Start();
        UNIT_ASSERT(startFuture2.HasValue());
        UNIT_ASSERT_STRINGS_EQUAL(startFuture2.GetValueSync().Response.GetError().GetDescription(), "Start was already called");
    }

    Y_UNIT_TEST(StartWithFailedCds) {
        TPQLib lib;
        auto consumer = lib.CreateConsumer(MakeFakeConsumerSettings(), new TCerrLogger(TLOG_DEBUG));
        auto startFuture = consumer->Start(TDuration::MilliSeconds(10));
        if (!GrpcV1EnabledByDefault()) {
            UNIT_ASSERT_STRING_CONTAINS(startFuture.GetValueSync().Response.GetError().GetDescription(), " cluster discovery attempts");
        }
        auto isDead = consumer->IsDead();
        isDead.Wait();
    }

    Y_UNIT_TEST(StartDeadline) {
        if (GrpcV1EnabledByDefault()) {
            return;
        }
        TPQLib lib;
        TConsumerSettings settings = MakeFakeConsumerSettings();
        auto consumer = lib.CreateConsumer(settings, new TCerrLogger(TLOG_DEBUG));
        auto startFuture = consumer->Start(TDuration::MilliSeconds(100));
        UNIT_ASSERT_EQUAL_C(startFuture.GetValueSync().Response.GetError().GetCode(), NErrorCode::CREATE_TIMEOUT, startFuture.GetValueSync().Response);
    }

    Y_UNIT_TEST(StartDeadlineAfterCds) {
        SDKTestSetup setup("StartDeadlineAfterCds");
        TPQLib lib;
        auto settings = MakeConsumerSettings(setup);
        settings.Topics[0] = "unknown_topic";
        auto consumer = lib.CreateConsumer(settings, new TCerrLogger(TLOG_DEBUG));
        auto startFuture = consumer->Start(TDuration::MilliSeconds(1));
        if (!GrpcV1EnabledByDefault()) {
            UNIT_ASSERT_STRINGS_EQUAL(startFuture.GetValueSync().Response.GetError().GetDescription(), "Start timeout");
        }
        auto isDead = consumer->IsDead();
        isDead.Wait();
    }

    void WorksWithSingleDc(bool oneDcIsUnavailable) {
        SDKTestSetup setup("WorksWithSingleDc", false);
        if (!oneDcIsUnavailable) {
            setup.SetSingleDataCenter(); // By default one Dc is fake in test setup. This call overrides it.
        }
        setup.Start();
        auto consumer = setup.StartConsumer(MakeConsumerSettings(setup));
        setup.WriteToTopic({"msg1", "msg2"});
        setup.ReadFromTopic({{"msg1", "msg2"}}, true, consumer.Get());
    }

    Y_UNIT_TEST(WorksWithSingleDc) {
        WorksWithSingleDc(false);
    }

    Y_UNIT_TEST(WorksWithSingleDcWhileSecondDcIsUnavailable) {
        WorksWithSingleDc(true);
    }

    Y_UNIT_TEST(FailsOnCommittingWrongCookie) {
        SDKTestSetup setup("FailsOnCommittingWrongCookie");
        auto consumer = setup.StartConsumer(MakeConsumerSettings(setup));
        setup.WriteToTopic({"msg1", "msg2"});
        auto nextMessage = consumer->GetNextMessage();
        UNIT_ASSERT_EQUAL(nextMessage.GetValueSync().Type, EMT_DATA);
        auto isDead = consumer->IsDead();
        consumer->Commit({nextMessage.GetValueSync().Response.GetData().GetCookie() + 1});
        UNIT_ASSERT_STRING_CONTAINS(isDead.GetValueSync().GetDescription(), "Wrong cookie");
    }

    // Test that consumer is properly connected with decompressing consumer.
    void Unpacks(bool unpack) {
        SDKTestSetup setup(TStringBuilder() << "Unpacks(" << (unpack ? "true)" : "false)"));
        setup.WriteToTopic({"msg"});
        auto settings = MakeConsumerSettings(setup);
        settings.Unpack = unpack;
        auto consumer = setup.StartConsumer(settings);
        auto readResponse = consumer->GetNextMessage().GetValueSync();
        UNIT_ASSERT_C(!readResponse.Response.HasError(), readResponse.Response);
        UNIT_ASSERT_C(readResponse.Response.HasData(), readResponse.Response);
        if (unpack) {
            UNIT_ASSERT_EQUAL_C(readResponse.Response.GetData().GetMessageBatch(0).GetMessage(0).GetMeta().GetCodec(), NPersQueueCommon::RAW, readResponse.Response);
            UNIT_ASSERT_STRINGS_EQUAL(readResponse.Response.GetData().GetMessageBatch(0).GetMessage(0).GetData(), "msg");
        } else {
            UNIT_ASSERT_EQUAL_C(readResponse.Response.GetData().GetMessageBatch(0).GetMessage(0).GetMeta().GetCodec(), NPersQueueCommon::GZIP, readResponse.Response);
        }
    }

    Y_UNIT_TEST(UnpacksIfNeeded) {
        Unpacks(true);
    }

    Y_UNIT_TEST(DoesNotUnpackIfNeeded) {
        Unpacks(false);
    }

    Y_UNIT_TEST(WorksWithManyDc) {
        SDKTestSetup setup1("WorksWithManyDc1", false);
        SDKTestSetup setup2("WorksWithManyDc2");
        SDKTestSetup setup3("WorksWithManyDc3", false);
        setup1.AddDataCenter("dc2", setup2);
        setup1.AddDataCenter("dc3", setup3);
        setup3.GetGrpcServerOptions().SetGRpcShutdownDeadline(TDuration::Seconds(1));
        setup3.Start();
        setup1.Start();

        // Check that consumer reads from all sources.
        setup1.WriteToTopic({"msg1", "msg2"});
        setup2.WriteToTopic({"msg3", "msg4"});
        setup3.WriteToTopic({"msg10", "msg11", "msg12", "msg13", "msg14", "msg15"});
        auto consumer = setup1.StartConsumer(MakeConsumerSettings(setup1));
        setup1.ReadFromTopic({{"msg1", "msg2"}, {"msg3", "msg4"}, {"msg10", "msg11", "msg12", "msg13", "msg14", "msg15"}}, true, consumer.Get());

        // Turn Dc off and check that consumer works.
        setup1.GetLog() << TLOG_INFO << "Turn off DataCenter dc3";
        setup3.ShutdownGRpc();
        setup1.WriteToTopic({"a", "b"});
        setup2.WriteToTopic({"x", "y", "z"});
        setup1.ReadFromTopic({{"a", "b"}, {"x", "y", "z"}}, true, consumer.Get());

        // Turn Dc on again and check work again.
        setup1.GetLog() << TLOG_INFO << "Turn on DataCenter dc3";
        setup3.EnableGRpc();
        setup1.WriteToTopic({"1", "2"});
        setup3.WriteToTopic({"42"});
        setup1.ReadFromTopic({{"1", "2"}, {"42"}}, true, consumer.Get());
    }
}

} // namespace NPersQueue
