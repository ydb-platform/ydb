#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/control_plane.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/persqueue_impl.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::inline Dev::NPersQueue {
    namespace {

        constexpr const char* ReplicationConsumerAttribute = "_ydb_replication_consumer";
        constexpr const char* MirrorConsumerOffsetsAttribute = "_ydb_mirror_consumer_offsets";
        constexpr const char* ConsumerName = "shared/mirror-from-source-to-shadow";

        template <class TRequest, class TSettings>
        void TestReplicationAttributesRoundTrip() {
            for (const bool enabled : {false, true}) {
                Ydb::PersQueue::V1::TopicSettings proto;
                (*proto.mutable_attributes())[ReplicationConsumerAttribute] = ConsumerName;
                (*proto.mutable_attributes())[MirrorConsumerOffsetsAttribute] = enabled ? "true" : "false";
                const TDescribeTopicResult::TTopicSettings description(proto);
                UNIT_ASSERT(description.ReplicationConsumer().has_value());
                UNIT_ASSERT_VALUES_EQUAL(*description.ReplicationConsumer(), ConsumerName);
                UNIT_ASSERT(description.MirrorConsumerOffsets().has_value());
                UNIT_ASSERT_VALUES_EQUAL(*description.MirrorConsumerOffsets(), enabled);

                // This is the conversion used by config-manager's Create/Alter executors.
                TSettings settings;
                settings.SetSettings(description);
                const auto request = TPersQueueClient::TImpl::MakePropsCreateOrAlterRequest<TRequest>("topic", settings);
                const auto& attributes = request.settings().attributes();
                UNIT_ASSERT_VALUES_EQUAL(attributes.at(ReplicationConsumerAttribute), ConsumerName);
                UNIT_ASSERT_VALUES_EQUAL(attributes.at(MirrorConsumerOffsetsAttribute), enabled ? "true" : "false");
            }
        }

        template <class TRequest, class TSettings>
        void TestAbsentReplicationAttributes() {
            const TDescribeTopicResult::TTopicSettings description(Ydb::PersQueue::V1::TopicSettings{});
            UNIT_ASSERT(!description.ReplicationConsumer().has_value());
            UNIT_ASSERT(!description.MirrorConsumerOffsets().has_value());

            TSettings settings;
            settings.ReplicationConsumer(ConsumerName).MirrorConsumerOffsets(true);
            settings.SetSettings(description);
            const auto request = TPersQueueClient::TImpl::MakePropsCreateOrAlterRequest<TRequest>("topic", settings);
            UNIT_ASSERT(!request.settings().attributes().contains(ReplicationConsumerAttribute));
            UNIT_ASSERT(!request.settings().attributes().contains(MirrorConsumerOffsetsAttribute));
        }

    } // namespace

    Y_UNIT_TEST_SUITE(TPersQueueReplicationAttributes) {
        Y_UNIT_TEST(CreateTopicPreservesReplicationAttributes) {
            TestReplicationAttributesRoundTrip<Ydb::PersQueue::V1::CreateTopicRequest, TCreateTopicSettings>();
        }

        Y_UNIT_TEST(AlterTopicPreservesReplicationAttributes) {
            TestReplicationAttributesRoundTrip<Ydb::PersQueue::V1::AlterTopicRequest, TAlterTopicSettings>();
        }

        Y_UNIT_TEST(CreateTopicDoesNotIntroduceAbsentReplicationAttributes) {
            TestAbsentReplicationAttributes<Ydb::PersQueue::V1::CreateTopicRequest, TCreateTopicSettings>();
        }

        Y_UNIT_TEST(AlterTopicDoesNotIntroduceAbsentReplicationAttributes) {
            TestAbsentReplicationAttributes<Ydb::PersQueue::V1::AlterTopicRequest, TAlterTopicSettings>();
        }

        Y_UNIT_TEST(FluentSettingsCanExplicitlyDisableOffsetMirroring) {
            TAlterTopicSettings settings;
            settings.ReplicationConsumer(ConsumerName).MirrorConsumerOffsets(false);
            const auto request = TPersQueueClient::TImpl::MakePropsCreateOrAlterRequest<Ydb::PersQueue::V1::AlterTopicRequest>("topic", settings);
            UNIT_ASSERT_VALUES_EQUAL(request.settings().attributes().at(ReplicationConsumerAttribute), ConsumerName);
            UNIT_ASSERT_VALUES_EQUAL(request.settings().attributes().at(MirrorConsumerOffsetsAttribute), "false");
        }

        Y_UNIT_TEST(EmptyAttributesKeepServerSemantics) {
            Ydb::PersQueue::V1::TopicSettings proto;
            (*proto.mutable_attributes())[ReplicationConsumerAttribute] = "";
            (*proto.mutable_attributes())[MirrorConsumerOffsetsAttribute] = "";
            TAlterTopicSettings settings;
            settings.SetSettings(TDescribeTopicResult::TTopicSettings(proto));
            const auto request = TPersQueueClient::TImpl::MakePropsCreateOrAlterRequest<Ydb::PersQueue::V1::AlterTopicRequest>("topic", settings);
            UNIT_ASSERT_VALUES_EQUAL(request.settings().attributes().at(ReplicationConsumerAttribute), "");
            UNIT_ASSERT_VALUES_EQUAL(request.settings().attributes().at(MirrorConsumerOffsetsAttribute), "false");
        }
    } // Y_UNIT_TEST_SUITE(TPersQueueReplicationAttributes)

} // namespace NYdb::inline Dev::NPersQueue
