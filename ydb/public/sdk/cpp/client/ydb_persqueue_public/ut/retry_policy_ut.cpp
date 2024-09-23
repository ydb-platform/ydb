#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/testing/unittest/registar.h>


using namespace NThreading;
using namespace NKikimr;
using namespace NKikimr::NPersQueueTests;
using namespace NPersQueue;

namespace NYdb::NPersQueue::NTests {

Y_UNIT_TEST_SUITE(RetryPolicy) {
    Y_UNIT_TEST(TWriteSession_TestPolicy) {
        TYdbPqWriterTestHelper helper(TEST_CASE_NAME);
        helper.Write(true);
        helper.Policy->Initialize(); // Thus ignoring possible early retries on "cluster initializing"
        auto doBreakDown = [&] () {
            helper.Policy->ExpectBreakDown();
            NThreading::TPromise<void> retriesPromise = NThreading::NewPromise();
            Cerr << "WAIT for retries...\n";
            helper.Policy->WaitForRetries(30, retriesPromise);
            Cerr << "KICK tablets\n";
            helper.Setup->KickTablets();

            auto f1 = helper.Write(false);
            auto f2 = helper.Write();

            auto retriesFuture = retriesPromise.GetFuture();
            retriesFuture.Wait();
            Cerr << "WAIT for retries done\n";

            NThreading::TPromise<void> repairPromise = NThreading::NewPromise();
            auto repairFuture = repairPromise.GetFuture();
            helper.Policy->WaitForRepair(repairPromise);


            Cerr << "ALLOW tablets\n";
            helper.Setup->AllowTablets();

            Cerr << "WAIT for repair\n";
            repairFuture.Wait();
            Cerr << "REPAIR done\n";
            f1.Wait();
            f2.Wait();
            helper.Write(true);
        };
        doBreakDown();
        doBreakDown();

    }
    Y_UNIT_TEST(TWriteSession_TestBrokenPolicy) {
        TYdbPqWriterTestHelper helper(TEST_CASE_NAME);
        helper.Write();
        helper.Policy->Initialize();
        helper.Policy->ExpectFatalBreakDown();
        helper.EventLoop->AllowStop();
        auto f1 = helper.Write(false);
        helper.Setup->KickTablets();
        helper.Write(false);

        helper.EventLoop->WaitForStop();
        UNIT_ASSERT(!f1.HasValue());
        helper.Setup = nullptr;

    };

    Y_UNIT_TEST(TWriteSession_RetryOnTargetCluster) {
        auto setup1 = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        SDKTestSetup setup2("RetryOnTargetCluster_Dc2");
        setup1->AddDataCenter("dc2", setup2, false);
        setup1->Start();
        auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
        auto settings = setup1->GetWriteSessionSettings();
        settings.ClusterDiscoveryMode(EClusterDiscoveryMode::On);
        settings.PreferredCluster("dc2");
        settings.AllowFallbackToOtherClusters(false);
        settings.RetryPolicy(retryPolicy);

        retryPolicy->Initialize();
        retryPolicy->ExpectBreakDown();

        auto& client = setup1->GetPersQueueClient();
        Cerr << "=== Create write session \n";
        auto writer = client.CreateWriteSession(settings);

        NThreading::TPromise<void> retriesPromise = NThreading::NewPromise();
        auto retriesFuture = retriesPromise.GetFuture();
        retryPolicy->WaitForRetries(3, retriesPromise);
        Cerr << "=== Wait retries\n";
        retriesFuture.Wait();

        Cerr << "=== Enable dc2\n";
        setup1->EnableDataCenter("dc2");

        NThreading::TPromise<void> repairPromise = NThreading::NewPromise();
        auto repairFuture = repairPromise.GetFuture();
        retryPolicy->WaitForRepair(repairPromise);
        Cerr << "=== Wait for repair\n";
        repairFuture.Wait();
        Cerr << "=== Close writer\n";
        writer->Close();
    }

    Y_UNIT_TEST(TWriteSession_SwitchBackToLocalCluster) {
        Cerr << "====Start test\n";

        auto setup1 = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        SDKTestSetup setup2("SwitchBackToLocalCluster", false);
        setup2.SetSingleDataCenter("dc2");
        setup2.AddDataCenter("dc1", *setup1, true);
        setup1->AddDataCenter("dc2", setup2, true);
        setup1->Start();
        setup2.Start(false);
        Cerr << "=== Start session 1\n";
        auto helper = MakeHolder<TYdbPqWriterTestHelper>("", nullptr, TString(), setup1);
        helper->Write(true);
        auto retryPolicy = helper->Policy;
        retryPolicy->Initialize();

        auto waitForReconnect = [&](bool enable) {
            Cerr << "=== Expect breakdown\n";
            retryPolicy->ExpectBreakDown();

            NThreading::TPromise<void> retriesPromise = NThreading::NewPromise();
            auto retriesFuture = retriesPromise.GetFuture();
            retryPolicy->WaitForRetries(1, retriesPromise);

            NThreading::TPromise<void> repairPromise = NThreading::NewPromise();
            auto repairFuture = repairPromise.GetFuture();
            retryPolicy->WaitForRepair(repairPromise);

            if (enable) {
                Cerr << "===Enabled DC1\n";
                setup1->EnableDataCenter("dc1");
                setup2.EnableDataCenter("dc1");
            } else {
                Cerr << "===Disabled DC1\n";
                setup1->DisableDataCenter("dc1");
                setup2.DisableDataCenter("dc1");
            }
            Sleep(TDuration::Seconds(5));

            retriesFuture.Wait();
            repairFuture.Wait();
        };
        Cerr << "===Wait for 1st reconnect\n";
        waitForReconnect(false);
        Cerr << "===Wait for 2nd reconnect\n";
        waitForReconnect(true);
    }

    Y_UNIT_TEST(TWriteSession_SeqNoShift) {
        auto setup1 = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false, TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_TRACE);
        SDKTestSetup setup2("SeqNoShift_Dc2", false, TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_TRACE);
        setup2.SetSingleDataCenter("dc2");
        setup2.AddDataCenter("dc1", *setup1, true);
        setup2.Start(true, false);
        setup1->AddDataCenter("dc2", setup2, true);
        setup1->Start(true, false);

        TString sourceId1 = SDKTestSetup::GetTestMessageGroupId() + "1";
        TString sourceId2 = SDKTestSetup::GetTestMessageGroupId() + "2";
        auto writer1 = MakeHolder<TYdbPqWriterTestHelper>("", nullptr, "dc1", setup1, sourceId1 , true);
        auto writer2 = MakeHolder<TYdbPqWriterTestHelper>("", nullptr, "dc1", setup1, sourceId2, true);

        auto settings = setup1->GetWriteSessionSettings();
        auto& client = setup1->GetPersQueueClient();

        //! Fill data in dc1 1 with SeqNo = 1..10 for 2 different SrcId
        Cerr << "===Write 10 messages into every writer\n";
        for (auto i = 0; i != 10; i++) {
            writer1->Write(true); // 1
            writer2->Write(true); // 1
        }
        Cerr << "===Messages were written\n";

        Cerr << "===Disable dc1\n";
        //! Leave only dc2 available
        writer1->Policy->ExpectBreakDown();
        writer2->Policy->ExpectBreakDown();
        setup1->DisableDataCenter("dc1");
        writer1->Policy->WaitForRetriesSync(1);
        writer2->Policy->WaitForRetriesSync(1);

        Cerr << "===Recreate writers\n";

        //! Re-create writers, kill previous sessions. New sessions will connect to dc2.
        writer1 = MakeHolder<TYdbPqWriterTestHelper>("", nullptr, TString(), setup1, sourceId1, true);
        writer2 = MakeHolder<TYdbPqWriterTestHelper>("", nullptr, TString(), setup1, sourceId2, true);

        //! Write some data and await confirmation - just to ensure sessions are started.
        Cerr << "===Write one message into every writer\n";
        writer1->Write(true);
        writer2->Write(true);
        Cerr << "===Messages were written\n";

        //! Leave no available DCs
        writer1->Policy->ExpectBreakDown();
        writer2->Policy->ExpectBreakDown();
        writer1->Policy->Initialize();
        writer2->Policy->Initialize();

        Cerr << "===Disable dc2\n";
        setup1->DisableDataCenter("dc2");
        Cerr << "===Wait for retries after initial dc2 shutdown\n";
        writer1->Policy->WaitForRetriesSync(1);
        writer2->Policy->WaitForRetriesSync(1);

        //! Put some data inflight. It cannot be written now, but SeqNo will be assigned.
        Cerr << "===Write four async message into every writer\n";
        for (auto i = 0; i != 3; i++) {
            writer1->Write(false);
            writer2->Write(false);
        }
        auto f1 = writer1->Write(false);
        auto f2 = writer2->Write(false);

        //! Enable DC1. Now writers gonna write collected data to DC1 having LastSeqNo = 10
        //! (because of data written in the very beginning), and inflight data has SeqNo assigned = 2..5,
        //! so the SeqNo shift takes place.
        Cerr << "===Enable dc2\n";
        setup1->EnableDataCenter("dc1");

        Cerr << "===Wait for writes to complete\n";
        f1.Wait();
        f2.Wait();
        Cerr << "===Messages were written\n";

        //! Writer1 is not used any more.
        writer1->EventLoop->AllowStop();
        writer1 = nullptr;

        Cerr << "===Writer 1 closed\n";
        writer2->Policy->ExpectBreakDown();
        writer2->Policy->Initialize();

        //! For the second writer, do switchback to dc2.
        Cerr << "===Disable dc1\n";
        setup1->DisableDataCenter("dc1");
        Cerr << "===Wait for retries after dc1 shutdown\n";
        writer2->Policy->WaitForRetriesSync(1);

        //! Put some data inflight again;
        Cerr << "===Write four async messages into writer2\n";
        for (auto i = 0; i != 3; i++) {
            writer2->Write(false);
        }
        f2 = writer2->Write(false);

        Cerr << "===Enable dc2\n";
        setup1->EnableDataCenter("dc2");

        f2.Wait();
        Cerr << "===Messages were written\n";

        writer2->EventLoop->AllowStop();
        writer2->Policy->ExpectBreakDown();
        writer2 = nullptr;

        Cerr << "===Enable dc1\n";
        setup1->EnableDataCenter("dc1");
        auto CheckSeqNo = [&] (const TString& dcName, ui64 expectedSeqNo) {
            settings.PreferredCluster(dcName);
            settings.AllowFallbackToOtherClusters(false);
            settings.RetryPolicy(nullptr); //switch to default policy;
            auto writer = client.CreateWriteSession(settings);
            auto seqNo = writer->GetInitSeqNo().GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(seqNo, expectedSeqNo);
            writer->Close(TDuration::Zero());
        };

        //!check SeqNo in both DC. For writer1 We expect 14 messages in DC1
        //! (10 written initially + 4 written after reconnect) and 1 message in DC2 (only initial message).
        settings.MessageGroupId(sourceId1);
        Cerr << "===Check SeqNo writer1, dc2\n";
        CheckSeqNo("dc2", 1);
        Cerr << "===Check SeqNo writer1, dc1\n";
        CheckSeqNo("dc1", 14);

        //! Check SeqNo for writer 2; Expect to have 6 messages on DC2 with MaxSeqNo = 6;
        settings.MessageGroupId(sourceId2);
        Cerr << "===Check SeqNo writer2 dc1\n";
        CheckSeqNo("dc1", 14);
        //! DC2 has no shift in SeqNo since 5 messages were written to dc 1.
        Cerr << "===Check SeqNo writer2 dc2\n";
        CheckSeqNo("dc2", 9);


        auto readSession = client.CreateReadSession(setup1->GetReadSessionSettings());

        bool stop = false;
        THashMap<TString, ui64> seqNoByClusterSrc1 = {
                {"dc1", 0},
                {"dc2", 0}
        };
        auto SeqNoByClusterSrc2 = seqNoByClusterSrc1;

        THashMap<TString, ui64> MsgCountByClusterSrc1 = {
                {"dc1", 14},
                {"dc2", 1}
        };
        THashMap<TString, ui64> MsgCountByClusterSrc2 = {
                {"dc1", 14},
                {"dc2", 5}
        };
        ui32 clustersPendingSrc1 = 2;
        ui32 clustersPendingSrc2 = 2;

        while (!stop && (clustersPendingSrc2 || clustersPendingSrc1)) {
            Cerr << "===Get event on client\n";
            auto event = *readSession->GetEvent(true);
            std::visit(TOverloaded {
                    [&](TReadSessionEvent::TDataReceivedEvent& event) {
                        Cerr << "===Data event\n";
                        auto& clusterName = event.GetPartitionStream()->GetCluster();
                        for (auto& message: event.GetMessages()) {
                            TString sourceId = message.GetMessageGroupId();
                            ui32 seqNo = message.GetSeqNo();
                            if (sourceId == sourceId1) {
                                UNIT_ASSERT_VALUES_EQUAL(seqNo, seqNoByClusterSrc1[clusterName] + 1);
                                seqNoByClusterSrc1[clusterName]++;
                                auto& msgRemaining = MsgCountByClusterSrc1[clusterName];
                                UNIT_ASSERT(msgRemaining > 0);
                                msgRemaining--;
                                if (!msgRemaining)
                                    clustersPendingSrc1--;
                            } else {
                                UNIT_ASSERT_VALUES_EQUAL(sourceId, sourceId2);
                                auto& prevSeqNo = SeqNoByClusterSrc2[clusterName];
                                if (clusterName == "dc1") {
                                    UNIT_ASSERT_VALUES_EQUAL(seqNo, prevSeqNo + 1);
                                    prevSeqNo++;
                                } else {
                                    UNIT_ASSERT_VALUES_EQUAL(clusterName, "dc2");
                                    if (prevSeqNo == 0) {
                                        UNIT_ASSERT_VALUES_EQUAL(seqNo, 1);
                                    } else if (prevSeqNo == 1) {
                                        UNIT_ASSERT_VALUES_EQUAL(seqNo, 6);
                                    } else {
                                        UNIT_ASSERT_VALUES_EQUAL(seqNo, prevSeqNo + 1);
                                    }
                                    prevSeqNo = seqNo;
                                }
                                auto& msgRemaining = MsgCountByClusterSrc2[clusterName];
                                UNIT_ASSERT(msgRemaining > 0);
                                msgRemaining--;
                                if (!msgRemaining)
                                    clustersPendingSrc2--;
                            }
                            message.Commit();
                        }
                    },
                    [&](TReadSessionEvent::TCommitAcknowledgementEvent&) {
                    },
                    [&](TReadSessionEvent::TCreatePartitionStreamEvent& event) {
                        event.Confirm();
                    },
                    [&](TReadSessionEvent::TDestroyPartitionStreamEvent& event) {
                        event.Confirm();
                    },
                    [&](TReadSessionEvent::TPartitionStreamStatusEvent&) {
                        Cerr << "===Status event\n";
                        UNIT_FAIL("Test does not support lock sessions yet");
                    },
                    [&](TReadSessionEvent::TPartitionStreamClosedEvent&) {
                        Cerr << "===Stream closed event\n";
                        UNIT_FAIL("Test does not support lock sessions yet");
                    },
                    [&](TSessionClosedEvent& event) {
                        Cerr << "===Got close event: " << event.DebugString();
                        stop = true;
                    }

            }, event);
        }
        UNIT_ASSERT_VALUES_EQUAL(clustersPendingSrc1 || clustersPendingSrc2, 0);
    }
    Y_UNIT_TEST(RetryWithBatching) {
        auto setup = std::make_shared<TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
        auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
        auto settings = setup->GetWriteSessionSettings()
            .BatchFlushInterval(TDuration::Seconds(1000)) // Batch on size, not on time.
            .BatchFlushSizeBytes(100)
            .RetryPolicy(retryPolicy);
        auto& client = setup->GetPersQueueClient();
        auto writer = client.CreateWriteSession(settings);
        auto event = *writer->GetEvent(true);
        Cerr << NYdb::NPersQueue::DebugString(event) << "\n";
        UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
        auto continueToken = std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken);
        TString message = "1234567890";
        ui64 seqNo = 0;
        setup->KickTablets();
        setup->WaitForTabletsDown();

        writer->Write(std::move(continueToken), message, ++seqNo);
        retryPolicy->ExpectBreakDown();
        retryPolicy->WaitForRetriesSync(3);
        while (seqNo < 10) {
            auto event = *writer->GetEvent(true);
            Cerr << NYdb::NPersQueue::DebugString(event) << "\n";
            UNIT_ASSERT(std::holds_alternative<TWriteSessionEvent::TReadyToAcceptEvent>(event));
            writer->Write(
                    std::move(std::get<TWriteSessionEvent::TReadyToAcceptEvent>(event).ContinuationToken),
                    message, ++seqNo
            );
        }

        setup->AllowTablets();
        retryPolicy->WaitForRepairSync();
        WaitMessagesAcked(writer, 1, seqNo);
    }
};
}; //NYdb::NPersQueue::NTests
