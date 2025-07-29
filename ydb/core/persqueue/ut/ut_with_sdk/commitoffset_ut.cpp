#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NPQ::NTest;

Y_UNIT_TEST_SUITE(CommitOffset) {

    void PrepareFlatTopic(TTopicSdkTestSetup& setup) {
        setup.CreateTopic();

        setup.Write("message-1");
        setup.Write("message-2");
        setup.Write("message-3");
    }

    void PrepareAutopartitionedTopic(TTopicSdkTestSetup& setup) {
        setup.CreateTopicWithAutoscale();

        // Creating partition hierarchy
        // 0 ──┬──> 1 ──┬──> 3
        //     │        └──> 4
        //     └──> 2
        //
        // Each partition has 3 messages

        setup.Write("message-0-1", 0);
        setup.Write("message-0-2", 0);
        setup.Write("message-0-3", 0);

        {
            ui64 txId = 1006;
            SplitPartition(setup, ++txId, 0, "a");
        }

        setup.Write("message-1-1", 1);
        setup.Write("message-1-2", 1);
        setup.Write("message-1-3", 1);

        setup.Write("message-2-1", 2);
        setup.Write("message-2-2", 2);
        setup.Write("message-2-3", 2);

        {
            ui64 txId = 1007;
            SplitPartition(setup, ++txId, 1, "0");
        }

        setup.Write("message-3-1", 3);
        setup.Write("message-3-2", 3);
        setup.Write("message-3-3", 3);

        setup.Write("message-4-1", 4);
        setup.Write("message-4-2", 4);
        setup.Write("message-4-3", 4);
    }

    ui64 GetCommittedOffset(TTopicSdkTestSetup& setup, size_t partition) {
        auto description = setup.DescribeConsumer();
        auto stats = description.GetPartitions().at(partition).GetPartitionConsumerStats();
        UNIT_ASSERT(stats);
        return stats->GetCommittedOffset();
    }

    Y_UNIT_TEST(Commit_Flat_WithWrongSession) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareFlatTopic(setup);

        {
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1, "wrong-read-session-id");
            UNIT_ASSERT_C(!result.IsSuccess(), "Commit doesn`t work with wrong session id");
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 0));
        }
    }

    Y_UNIT_TEST(Commit_Flat_WithWrongSession_ToPast) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareFlatTopic(setup);

        {
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 2);
            UNIT_ASSERT_C(result.IsSuccess(), "Commited without session id. It is reset mode");
            UNIT_ASSERT_VALUES_EQUAL(2, GetCommittedOffset(setup, 0));
        }

        {
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 0, "wrong-read-session-id");
            UNIT_ASSERT_C(!result.IsSuccess(), "Commit doesn`t work with wrong session id");
            UNIT_ASSERT_VALUES_EQUAL(2, GetCommittedOffset(setup, 0));
        }
    }

    Y_UNIT_TEST(Commit_WithoutSession_TopPast) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        auto status = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "The consumer has just started reading the inactive partition and he can commit");

        status = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1);
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "A consumer who has not read to the end can commit messages forward.");

        status = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "A consumer who has not read to the end can commit messages back.");

        status = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 3);
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "The consumer can commit at the end of the inactive partition.");

        status = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL_C(NYdb::EStatus::SUCCESS, status.GetStatus(), "The consumer can commit an offset for inactive, read-to-the-end partitions.");
    }

    Y_UNIT_TEST(Commit_WithWrongSession_ToParent) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);
        setup.CreateTopicWithAutoscale();

        {
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1);
            UNIT_ASSERT_C(result.IsSuccess(), "Commited without session id. It is reset mode");
            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 0));
        }

        {
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 1, 1);
            UNIT_ASSERT_C(result.IsSuccess(), "Commited without session id. It is reset mode");
            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 1));
        }

        {
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 0, "wrong-read-session-id");
            UNIT_ASSERT_C(!result.IsSuccess(), "Commit doesn`t work with wrong session id");
            UNIT_ASSERT_VALUES_EQUAL_C(3, GetCommittedOffset(setup, 0), "Offset doesn`t changed");
            UNIT_ASSERT_VALUES_EQUAL_C(1, GetCommittedOffset(setup, 1), "Offset doesn`t changed");
        }
    }

    Y_UNIT_TEST(Commit_WithoutSession_ParentNotFinished) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        {
            // Commit parent partition to non end
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1);
            UNIT_ASSERT(result.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
        }

        {
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 1, 1);
            UNIT_ASSERT(result.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
        }
    }

    Y_UNIT_TEST(Commit_WithoutSession_ToPastParentPartition) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        {
            // Commit child partition to non end
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 3, 1);
            UNIT_ASSERT(result.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 3));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
        }

        {
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 1, 1);
            UNIT_ASSERT(result.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
        }
    }

    Y_UNIT_TEST(Commit_WithSession_ParentNotFinished_SameSession) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        {
            // Commit parent partition to non end
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1);
            UNIT_ASSERT(result.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
        }

        {
            auto r = setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto& x) {
                return x.GetMessages().back().GetData() != "message-3-3";
            });

            // Commit parent to middle
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 1, 1, r.StartPartitionSessionEvents.front().GetPartitionSession()->GetReadSessionId());
            UNIT_ASSERT(result.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
        }
    }

    Y_UNIT_TEST(Commit_WithSession_ParentNotFinished_OtherSession) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);
        TTopicClient client(setup.MakeDriver());

        auto r0 = setup.Read(TEST_TOPIC, TEST_CONSUMER, [](auto&) { return false; }, 0);
        auto r1 = setup.Read(TEST_TOPIC, TEST_CONSUMER, [](auto&) { return false; }, 1);

        auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 1, 1,
            r1.StartPartitionSessionEvents.back().GetPartitionSession()->GetReadSessionId());
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 0));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 1));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
    }

    Y_UNIT_TEST(Commit_WithSession_ParentNotFinished_OtherSession_ParentCommittedToEnd) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);
        TTopicClient client(setup.MakeDriver());

        setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 3);

        auto r0 = setup.Read(TEST_TOPIC, TEST_CONSUMER, [](auto&) { return false; }, 0);
        auto r1 = setup.Read(TEST_TOPIC, TEST_CONSUMER, [](auto&) { return false; }, 1);

        auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 1, 1,
            r1.StartPartitionSessionEvents.back().GetPartitionSession()->GetReadSessionId());
        UNIT_ASSERT(result.IsSuccess());

        UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
        UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 1));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
    }

    Y_UNIT_TEST(Commit_WithSession_ToPastParentPartition) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        {
            // Commit parent partition to non end
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 3, 1);
            UNIT_ASSERT(result.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 3));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
        }

        {
            auto r = setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto&) {
                return false;
            });

            // Commit parent to middle
            auto result = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 1, 1, r.StartPartitionSessionEvents.front().GetPartitionSession()->GetReadSessionId());
            UNIT_ASSERT(result.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
            UNIT_ASSERT_VALUES_EQUAL(1, GetCommittedOffset(setup, 3));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
        }
    }

    Y_UNIT_TEST(Commit_FromSession_ToNewChild_WithoutCommitToParent) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopicWithAutoscale();

        setup.Write("message-0-0", 0);
        setup.Write("message-0-1", 0);
        setup.Write("message-0-2", 0);

        {
            bool committed = false;

            auto r = setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto& x) {
                for (auto & m: x.GetMessages()) {
                    if (x.GetPartitionSession()->GetPartitionId() == 0 && m.GetOffset() == 1) {
                        ui64 txId = 1006;
                        SplitPartition(setup, ++txId, 0, "a");

                        setup.Write("message-1-0", 1);
                        setup.Write("message-1-1", 1);
                        setup.Write("message-1-2", 1);
                    } else if (x.GetPartitionSession()->GetPartitionId() == 1 && m.GetOffset() == 0) {
                        m.Commit();
                        committed = true;
                        return false;
                    }
                }

                return true;
            });

            UNIT_ASSERT(committed);

            Sleep(TDuration::Seconds(3));

            // Commit hasn`t applyed because messages from the parent partitions has`t been committed
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
        }
    }

    Y_UNIT_TEST(PartitionSplit_OffsetCommit) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        {
            static constexpr size_t commited = 2;
            auto status = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 1, commited);
            UNIT_ASSERT(status.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL_C(3, GetCommittedOffset(setup, 0), "Must be commited to the partition end because it is the parent");
            UNIT_ASSERT_VALUES_EQUAL(commited, GetCommittedOffset(setup, 1));
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
        }

        {
            static constexpr size_t commited = 3;
            auto status = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, commited);
            UNIT_ASSERT(status.IsSuccess());

            UNIT_ASSERT_VALUES_EQUAL(commited, GetCommittedOffset(setup, 0));
            UNIT_ASSERT_VALUES_EQUAL_C(0, GetCommittedOffset(setup, 1), "Must be commited to the partition begin because it is the child");
            UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));

        }
    }

    Y_UNIT_TEST(DistributedTxCommit) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        auto count = 0;
        const auto expected = 15;

        auto result = setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto& x) {
            auto& messages = x.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                ++count;
                auto& message = messages[i];
                Cerr << "SESSION EVENT read message: " << count << " from partition: " << message.GetPartitionSession()->GetPartitionId() << Endl << Flush;
                message.Commit();
            }

            return true;
        });

        UNIT_ASSERT(result.Timeout);
        UNIT_ASSERT_VALUES_EQUAL(count, expected);

        UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
        UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 1));
        UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 2));
        UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 3));
        UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 4));
    }

    Y_UNIT_TEST(DistributedTxCommit_ChildFirst) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        auto count = 0;
        const auto expected = 15;

        std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> partition0Messages;

        auto result = setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto& x) {
            auto& messages = x.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];
                count++;
                int partitionId = message.GetPartitionSession()->GetPartitionId();
                Cerr << "SESSION EVENT read message: " << count << " from partition: " << partitionId << Endl << Flush;
                if (partitionId == 1) {
                    // Commit messages from partition 1 immediately
                    message.Commit();
                } else if (partitionId == 0) {
                    // Store messages from partition 0 for later
                    partition0Messages.push_back(message);
                }
            }

            return true;
        });

        UNIT_ASSERT(result.Timeout);
        UNIT_ASSERT_VALUES_EQUAL(count, expected);

        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 0));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 1));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));

        for (auto& message : partition0Messages) {
            message.Commit();
        }

        Sleep(TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 0));
        UNIT_ASSERT_VALUES_EQUAL(3, GetCommittedOffset(setup, 1));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 2));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 3));
        UNIT_ASSERT_VALUES_EQUAL(0, GetCommittedOffset(setup, 4));
    }

    Y_UNIT_TEST(DistributedTxCommit_CheckSessionResetAfterCommit) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        std::unordered_map<std::string, size_t> counters;

        auto result = setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto& x) {
            auto& messages = x.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];
                message.Commit();

                auto count = ++counters[message.GetData()];

                // check we get this SeqNo two times
                if (message.GetData() == "message-0-3" && count == 1) {
                    Sleep(TDuration::MilliSeconds(300));
                    auto status = setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, 1);
                    UNIT_ASSERT(status.IsSuccess());
                }
            }

            return true;
        });

        UNIT_ASSERT_VALUES_EQUAL_C(1, counters["message-0-1"], "Message must be read 1 times because reset commit to offset 3, but 0 message has been read " << counters["message-0-1"] << " times") ;
        UNIT_ASSERT_VALUES_EQUAL_C(2, counters["message-0-2"], "Message must be read 2 times because reset commit to offset 1, but 1 message has been read " << counters["message-0-2"] << " times") ;
        UNIT_ASSERT_VALUES_EQUAL_C(2, counters["message-0-3"], "Message must be read 2 times because reset commit to offset 1, but 2 message has been read " << counters["message-0-3"] << " times") ;

        {
            auto s = result.StartPartitionSessionEvents[0];
            UNIT_ASSERT_VALUES_EQUAL(0, s.GetPartitionSession()->GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(0, s.GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(3, s.GetEndOffset());
        }
        {
            auto s = result.StartPartitionSessionEvents[3];
            UNIT_ASSERT_VALUES_EQUAL(0, s.GetPartitionSession()->GetPartitionId());
            UNIT_ASSERT_VALUES_EQUAL(1, s.GetCommittedOffset());
            UNIT_ASSERT_VALUES_EQUAL(3, s.GetEndOffset());
        }
    }

    Y_UNIT_TEST(DistributedTxCommit_CheckOffsetCommitForDifferentCases) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        auto commit = [&](const std::string& sessionId, ui64 offset) {
            return setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, offset, sessionId);
        };

        auto commitSent = false;
        TString readSessionId = "";

        setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto& x) {
            auto& messages = x.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];
                if (commitSent) {
                    // read session not changed
                    UNIT_ASSERT_EQUAL(readSessionId, message.GetPartitionSession()->GetReadSessionId());
                }

                // check we NOT get this SeqNo two times
                if (message.GetData() == "message-0-2") {
                    if (!commitSent) {
                        commitSent = true;

                        readSessionId = message.GetPartitionSession()->GetReadSessionId();

                        {
                            auto status = commit(readSessionId, 3);
                            UNIT_ASSERT(status.IsSuccess());
                            UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, 0), 3);
                        }

                        {
                            // must be ignored, because commit to past
                            auto status = commit(readSessionId, 0);
                            UNIT_ASSERT(status.IsSuccess());
                            UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, 0), 3);
                        }

                        {
                            // must be ignored, because wrong sessionid
                            auto status = commit("random session", 0);
                            UNIT_ASSERT(!status.IsSuccess());
                            Sleep(TDuration::MilliSeconds(500));
                            UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, 0), 3);
                        }
                    } else {
                        UNIT_ASSERT(false);
                    }
                } else {
                    message.Commit();
                }
            }

            return true;
        });
    }

    Y_UNIT_TEST(DistributedTxCommit_Flat_CheckOffsetCommitForDifferentCases) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareFlatTopic(setup);

        auto commit = [&](const std::string& sessionId, ui64 offset) {
            return setup.Commit(TEST_TOPIC, TEST_CONSUMER, 0, offset, sessionId);
        };

        auto commitSent = false;
        TString readSessionId = "";

        setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto& x) {
            auto& messages = x.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];

                if (commitSent) {
                    // read session not changed
                    UNIT_ASSERT_EQUAL(readSessionId, message.GetPartitionSession()->GetReadSessionId());
                }

                // check we NOT get this message two times
                if (message.GetData() == "message-0-2") {
                    if (!commitSent) {
                        commitSent = true;

                        readSessionId = message.GetPartitionSession()->GetReadSessionId();

                        {
                            auto status = commit(readSessionId, 3);
                            UNIT_ASSERT(status.IsSuccess());
                            UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, 0), 3);
                        }

                        {
                            // must be ignored, because commit to past
                            auto status = commit(readSessionId, 0);
                            UNIT_ASSERT(status.IsSuccess());
                            UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, 0), 3);
                        }

                        {
                            // must be ignored, because wrong sessionid
                            auto status = commit("random session", 0);
                            UNIT_ASSERT(!status.IsSuccess());
                            Sleep(TDuration::MilliSeconds(500));
                            UNIT_ASSERT_VALUES_EQUAL(GetCommittedOffset(setup, 0), 3);
                        }
                    } else {
                        UNIT_ASSERT(false);
                    }
                } else {
                    message.Commit();
                }
            }

            return true;
        });
    }

    Y_UNIT_TEST(DistributedTxCommit_LongReadSession) {
        TTopicSdkTestSetup setup = CreateSetup();
        PrepareAutopartitionedTopic(setup);

        std::vector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent> messages;

        auto result = setup.Read(TEST_TOPIC, TEST_CONSUMER, [&](auto& x) {
            messages.push_back(x);
            return true;
        });

        {
            Cerr << ">>>>> Alter topic" << Endl << Flush;
            TTopicClient client(setup.MakeDriver());
            TAlterTopicSettings settings;
            settings.SetRetentionPeriod(TDuration::Hours(1));
            client.AlterTopic(TEST_TOPIC, settings).GetValueSync();
        }

        // Wait recheck acl happened
        messages[0].GetMessages()[0].Commit(); // trigger recheck
        Sleep(TDuration::Seconds(5));

        Cerr << ">>>>> Commit message" << Endl << Flush;

        bool first = true;
        for (auto& e : messages) {
            for (auto& m :e.GetMessages()) {
                if (first) {
                    first = false;
                    continue;
                }
                m.Commit();
            }
        }

        // Wait commit happened
        Sleep(TDuration::Seconds(5));

        result.Reader->Close();
    }

}

} // namespace NKikimr
