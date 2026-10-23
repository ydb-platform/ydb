#include "ut_utils/topic_sdk_test_setup.h"
#include "reader_metrics_test_utils.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/offsets_collector.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {
    namespace {


        class TTestPartitionSession final: public TPartitionSession {
        public:
            explicit TTestPartitionSession(std::string topicPath) {
                PartitionSessionId = 1;
                TopicPath = std::move(topicPath);
                ReadSessionId = "read-session";
                PartitionId = 0;
            }

            void RequestStatus() override {
            }
        };

        class TCapturingTransaction final: public TTransactionBase {
        public:
            explicit TCapturingTransaction(TTransactionBase& tx) {
                SessionId_ = &tx.GetSessionId();
                TxId_ = &tx.GetId();
            }

            void AddPrecommitCallback(TPrecommitTransactionCallback callback) override {
                Precommit_ = std::move(callback);
            }

            void AddOnFailureCallback(TOnFailureTransactionCallback) override {
            }

            TPrecommitTransactionCallback TakePrecommit() {
                return std::move(Precommit_);
            }

        private:
            TPrecommitTransactionCallback Precommit_;
        };

        TReadSessionEvent::TDataReceivedEvent MakeDataEventAt(
            const std::string& topicPath, std::uint64_t offset, std::uint64_t logicalMessageCount)
        {
            auto partitionSession = MakeIntrusive<TTestPartitionSession>(topicPath);
            TReadSessionEvent::TDataReceivedEvent::TMessageInformation messageInfo(
                offset, "producer", 0, TInstant::Now(), TInstant::Now(),
                TWriteSessionMeta::TPtr(), TMessageMeta::TPtr(), 0, "group", logicalMessageCount);
            std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
            messages.emplace_back("payload", std::exception_ptr{}, std::move(messageInfo), partitionSession);
            return TReadSessionEvent::TDataReceivedEvent(
                std::move(messages),
                std::vector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>{},
                partitionSession);
        }

        TReadSessionEvent::TDataReceivedEvent MakeDataEvent(
            const std::string& topicPath, std::uint64_t logicalMessageCount)
        {
            return MakeDataEventAt(topicPath, 0, logicalMessageCount);
        }

        NMetrics::TLabels MakeLabels(
            const TTopicSdkTestSetup& setup,
            const std::string& topic,
            const std::string& consumer,
            const std::string& reader)
        {
            return {
                {"endpoint", setup.GetEndpoint()},
                {"database", setup.GetDatabase()},
                {"topic", topic},
                {"consumer", consumer},
                {"reader.name", reader},
            };
        }

        void ConfirmStartEvent(std::optional<TReadSessionEvent::TEvent>& event) {
            UNIT_ASSERT(event);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event);
            UNIT_ASSERT(start);
            start->Confirm();
        }

        void CheckPullDelivery(bool batch, bool useSettings) {
            TTopicSdkTestSetup setup("DeliveredMessages.Pull");
            setup.Write("payload");

            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);

            const std::string topic = setup.GetTopicPath();
            const std::string consumer = setup.GetConsumerName();
            const std::string reader = "pull-reader";
            auto session = client.CreateReadSession(TReadSessionSettings()
                                                        .ConsumerName(consumer)
                                                        .ReaderName(reader)
                                                        .AppendTopics(TTopicReadSettings(topic)));
            const auto counter = registry->Find(
                "ydb.topic.reader.delivered.messages", MakeLabels(setup, topic, consumer, reader));
            UNIT_ASSERT(counter);

            auto pull = [&] {
                const auto settings = TReadSessionGetEventSettings().Block(false).MaxEventsCount(1);
                if (batch) {
                    return useSettings ? session->GetEvents(settings) : session->GetEvents(false, 1);
                }
                auto event = useSettings ? session->GetEvent(settings) : session->GetEvent(false);
                std::vector<TReadSessionEvent::TEvent> events;
                if (event) {
                    events.push_back(std::move(*event));
                }
                return events;
            };

            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto startEvents = pull();
            UNIT_ASSERT_VALUES_EQUAL(startEvents.size(), 1);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&startEvents.front());
            UNIT_ASSERT(start);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
            start->Confirm();

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            // Readiness has been signalled, but no data has left the pull queue yet.
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
            auto dataEvents = pull();
            UNIT_ASSERT_VALUES_EQUAL(dataEvents.size(), 1);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&dataEvents.front());
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(data->GetMessages().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);

            data->Commit();
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto ackEvents = pull();
            UNIT_ASSERT_VALUES_EQUAL(ackEvents.size(), 1);
            UNIT_ASSERT(std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&ackEvents.front()));
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);
            UNIT_ASSERT(pull().empty());
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);

            session->Close(TDuration::Seconds(5));
            session.reset();
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);
            driver.Stop(true);
        }

        void CheckCommonHandlerDelivery(bool useDataHandler, bool destroySessionBeforeCallback = false, bool cancelCallback = false) {
            TTopicSdkTestSetup setup("DeliveredMessages.CommonHandler");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);

            const std::string topic = setup.GetTopicPath();
            const std::string consumer = setup.GetConsumerName();
            const std::string reader = "common-reader";
            const auto labels = MakeLabels(setup, topic, consumer, reader);
            std::uint64_t dataHandlerCalls = 0;
            std::uint64_t commonDataCalls = 0;
            auto checkDelivery = [registry, labels](TReadSessionEvent::TDataReceivedEvent& event) {
                const auto counter = registry->Find("ydb.topic.reader.delivered.messages", labels);
                UNIT_ASSERT(counter);
                UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);
                UNIT_ASSERT_VALUES_EQUAL(event.GetMessages().size(), 1);
            };
            auto executor = std::make_shared<TManualExecutor>();
            auto handlers = TReadSessionSettings::TEventHandlers()
                                .StartPartitionSessionHandler([](TReadSessionEvent::TStartPartitionSessionEvent& event) {
                                    event.Confirm();
                                })
                                .CommonHandler([&commonDataCalls, checkDelivery](TReadSessionEvent::TEvent& event) {
                                    if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
                                        ++commonDataCalls;
                                        checkDelivery(*data);
                                    }
                                })
                                .HandlersExecutor(executor);
            if (useDataHandler) {
                handlers.DataReceivedHandler([&dataHandlerCalls, checkDelivery](
                                                 TReadSessionEvent::TDataReceivedEvent& event) {
                    ++dataHandlerCalls;
                    checkDelivery(event);
                });
            }
            auto session = client.CreateReadSession(TReadSessionSettings()
                                                        .ConsumerName(consumer)
                                                        .ReaderName(reader)
                                                        .AppendTopics(TTopicReadSettings(topic))
                                                        .EventHandlers(handlers));
            const auto counter = registry->Find("ydb.topic.reader.delivered.messages", labels);
            UNIT_ASSERT(counter);
            UNIT_ASSERT(executor->WaitForTask());
            executor->RunOne(); // Confirm the partition start via its dedicated handler.
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
            UNIT_ASSERT(executor->WaitForTask());
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
            if (destroySessionBeforeCallback) {
                session->Close(TDuration::Zero());
                session.reset();
                UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
            }
            if (cancelCallback) {
                executor->Discard();
                UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
                UNIT_ASSERT_VALUES_EQUAL(dataHandlerCalls, 0);
                UNIT_ASSERT_VALUES_EQUAL(commonDataCalls, 0);
                executor->Stop();
                driver.Stop(true);
                return;
            }
            executor->RunOne();
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(dataHandlerCalls, useDataHandler ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(commonDataCalls, useDataHandler ? 0 : 1);

            if (session) {
                session->Close(TDuration::Seconds(5));
                session.reset();
            }
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);
            executor->Stop();
            driver.Stop(true);
        }

    } // anonymous namespace

    Y_UNIT_TEST_SUITE(TReaderMetricsTest) {
        Y_UNIT_TEST(CancelledDataCallbackIsNotDelivery) {
            CheckCommonHandlerDelivery(true, true, true);
        }

        Y_UNIT_TEST(CancelledCommonCallbackIsNotDelivery) {
            CheckCommonHandlerDelivery(false, true, true);
        }

        Y_UNIT_TEST(PartialPullAndUnreadCleanupDoNotInventDelivery) {
            for (bool unread : {false, true}) {
                TTopicSdkTestSetup setup("DeliveredMessages.Partial");
                for (int i = 0; i < 3; ++i) {
                    setup.Write("payload");
                }
                auto registry = std::make_shared<TRecordingMetricRegistry>();
                auto config = setup.MakeDriverConfig();
                config.SetMetricRegistry(registry);
                TDriver driver(std::move(config));
                TTopicClient client(driver);
                auto session = client.CreateReadSession(TReadSessionSettings()
                                                            .ConsumerName(setup.GetConsumerName())
                                                            .ReaderName("partial-reader")
                                                            .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
                const auto labels = MakeLabels(
                    setup, setup.GetTopicPath(), setup.GetConsumerName(), "partial-reader");
                auto counter = registry->Find("ydb.topic.reader.delivered.messages", labels);
                auto received = registry->Find("ydb.topic.reader.received.messages", labels);
                UNIT_ASSERT(counter);
                UNIT_ASSERT(received);
                UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
                auto start = session->GetEvent(false);
                ConfirmStartEvent(start);
                // Wait until all messages are received before checking delivery.
                UNIT_ASSERT(received->WaitForValue(3));
                UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
                UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
                if (!unread) {
                    for (ui64 count = 1; count <= 3; ++count) {
                        UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
                        auto event = session->GetEvent(false, 1);
                        UNIT_ASSERT(event);
                        auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
                        UNIT_ASSERT(data);
                        UNIT_ASSERT_VALUES_EQUAL(data->GetMessages().size(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(counter->Value(), count);
                    }
                }
                session->Close(TDuration::Seconds(5));
                session.reset();
                UNIT_ASSERT_VALUES_EQUAL(counter->Value(), unread ? 0 : 3);
                driver.Stop(true);
            }
        }

        Y_UNIT_TEST(RealTransactionAndPreparationFailureForBothSettingsOverloads) {
            class TObservingPreparation final: public TTransactionBase {
            public:
                TObservingPreparation(TTransactionBase& tx, std::function<void()> observe)
                    : Tx_(tx)
                    , Observe_(std::move(observe))
                {
                    SessionId_ = &tx.GetSessionId();
                    TxId_ = &tx.GetId();
                }

                void AddPrecommitCallback(TPrecommitTransactionCallback callback) override {
                    Observe_();
                    Tx_.AddPrecommitCallback(std::move(callback));
                }

                void AddOnFailureCallback(TOnFailureTransactionCallback callback) override {
                    Observe_();
                    Tx_.AddOnFailureCallback(std::move(callback));
                }

            private:
                TTransactionBase& Tx_;
                std::function<void()> Observe_;
            };

            class TFailingPreparation final: public TTransactionBase {
            public:
                TFailingPreparation(TTransactionBase& tx, std::function<void()> beforeFailure)
                    : BeforeFailure_(std::move(beforeFailure))
                {
                    SessionId_ = &tx.GetSessionId();
                    TxId_ = &tx.GetId();
                }
                void AddPrecommitCallback(TPrecommitTransactionCallback) override {
                    BeforeFailure_();
                    throw std::runtime_error("controlled Tx preparation failure");
                }
                void AddOnFailureCallback(TOnFailureTransactionCallback) override {
                    BeforeFailure_();
                    throw std::runtime_error("controlled Tx preparation failure");
                }

            private:
                std::function<void()> BeforeFailure_;
            };
            for (bool batch : {false, true}) {
                for (bool fail : {false, true}) {
                    for (bool rollback : {false, true}) {
                        TTopicSdkTestSetup setup("DeliveredMessages.Transaction");
                        setup.Write("payload");
                        auto registry = std::make_shared<TRecordingMetricRegistry>();
                        auto config = setup.MakeDriverConfig();
                        config.SetMetricRegistry(registry);
                        TDriver driver(std::move(config));
                        TTopicClient client(driver);
                        NTable::TTableClient tables(driver);
                        auto tableResult = tables.GetSession().ExtractValueSync();
                        UNIT_ASSERT_C(tableResult.IsSuccess(), tableResult.GetIssues().ToString());
                        auto txResult = tableResult.GetSession().BeginTransaction().ExtractValueSync();
                        UNIT_ASSERT_C(txResult.IsSuccess(), txResult.GetIssues().ToString());
                        auto tx = txResult.GetTransaction();
                        const auto labels = MakeLabels(
                            setup, setup.GetTopicPath(), setup.GetConsumerName(), "tx-reader");
                        const auto checkNotDelivered = [registry, labels] {
                            const auto counter = registry->Find("ydb.topic.reader.delivered.messages", labels);
                            UNIT_ASSERT(counter);
                            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
                        };
                        TObservingPreparation observingTx(tx, checkNotDelivered);
                        TFailingPreparation failingTx(tx, checkNotDelivered);
                        auto session = client.CreateReadSession(TReadSessionSettings()
                                                                    .ConsumerName(setup.GetConsumerName())
                                                                    .ReaderName("tx-reader")
                                                                    .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
                        auto counter = registry->Find("ydb.topic.reader.delivered.messages", labels);
                        UNIT_ASSERT(counter);
                        UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
                        auto start = session->GetEvent(false);
                        ConfirmStartEvent(start);
                        UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
                        checkNotDelivered();
                        auto commitCounter = registry->Find("ydb.topic.reader.commit.queued", labels);
                        UNIT_ASSERT(commitCounter);
                        auto settings = TReadSessionGetEventSettings()
                                            .Block(false)
                                            .MaxEventsCount(1)
                                            .Tx(fail ? static_cast<TTransactionBase&>(failingTx)
                                                     : static_cast<TTransactionBase&>(observingTx));
                        auto pull = [&] {
                            if (batch) {
                                auto events = session->GetEvents(settings);
                                UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
                                UNIT_ASSERT(std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(events.front()));
                            } else {
                                auto event = session->GetEvent(settings);
                                UNIT_ASSERT(event);
                                UNIT_ASSERT(std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(*event));
                            }
                        };
                        if (fail) {
                            UNIT_ASSERT_EXCEPTION(pull(), std::runtime_error);
                        } else {
                            pull();
                        }
                        UNIT_ASSERT_VALUES_EQUAL(counter->Value(), fail ? 0 : 1);
                        if (!fail) {
                            if (rollback) {
                                const auto rollbackResult = tx.Rollback().ExtractValueSync();
                                UNIT_ASSERT_C(rollbackResult.IsSuccess(), rollbackResult.GetIssues().ToString());
                            } else {
                                const auto commitResult = tx.Commit().ExtractValueSync();
                                UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());
                            }
                        }
                        UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), (!fail && !rollback) ? 1 : 0);
                        session->Close(TDuration::Seconds(5));
                        session.reset();
                        driver.Stop(true);
                    }
                }
            }
        }

        Y_UNIT_TEST(RealTransactionAsyncFailureKeepsCommitQueued) {
            TTopicSdkTestSetup setup("DeliveredMessages.TransactionAsyncFailure");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            NTable::TTableClient tables(driver);
            auto tableResult = tables.GetSession().ExtractValueSync();
            UNIT_ASSERT_C(tableResult.IsSuccess(), tableResult.GetIssues().ToString());
            auto txResult = tableResult.GetSession().BeginTransaction().ExtractValueSync();
            UNIT_ASSERT_C(txResult.IsSuccess(), txResult.GetIssues().ToString());
            auto tx = txResult.GetTransaction();
            const auto labels = MakeLabels(
                setup, setup.GetTopicPath(), setup.GetConsumerName(), "tx-async-reader");
            auto session = client.CreateReadSession(TReadSessionSettings()
                                                        .ConsumerName(setup.GetConsumerName())
                                                        .ReaderName("tx-async-reader")
                                                        .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
            auto commitCounter = registry->Find("ydb.topic.reader.commit.queued", labels);
            UNIT_ASSERT(commitCounter);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto start = session->GetEvent(false);
            ConfirmStartEvent(start);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto settings = TReadSessionGetEventSettings()
                                .Block(false)
                                .MaxEventsCount(1)
                                .Tx(static_cast<TTransactionBase&>(tx));
            auto event = session->GetEvent(settings);
            UNIT_ASSERT(event);
            UNIT_ASSERT(std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(*event));
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 0);
            session.reset();
            driver.Stop(true);
            const auto result = tx.Commit().ExtractValueSync();
            UNIT_ASSERT(!result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 1);
            session.reset();
        }

        Y_UNIT_TEST(RealTransactionCommitQueuedAddFailurePreservesTransactionResult) {
            TTopicSdkTestSetup setup("DeliveredMessages.TransactionAddFailure");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>(TRecordingMetricRegistry::EFailure::Add);
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            NTable::TTableClient tables(driver);
            auto tableResult = tables.GetSession().ExtractValueSync();
            UNIT_ASSERT_C(tableResult.IsSuccess(), tableResult.GetIssues().ToString());
            auto txResult = tableResult.GetSession().BeginTransaction().ExtractValueSync();
            UNIT_ASSERT_C(txResult.IsSuccess(), txResult.GetIssues().ToString());
            auto tx = txResult.GetTransaction();
            const auto labels = MakeLabels(
                setup, setup.GetTopicPath(), setup.GetConsumerName(), "tx-add-failure-reader");
            auto session = client.CreateReadSession(TReadSessionSettings()
                                                        .ConsumerName(setup.GetConsumerName())
                                                        .ReaderName("tx-add-failure-reader")
                                                        .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
            auto commitCounter = registry->Find("ydb.topic.reader.commit.queued", labels);
            UNIT_ASSERT(commitCounter);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto start = session->GetEvent(false);
            ConfirmStartEvent(start);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto event = session->GetEvent(
                TReadSessionGetEventSettings()
                    .Block(false)
                    .MaxEventsCount(1)
                    .Tx(static_cast<TTransactionBase&>(tx)));
            UNIT_ASSERT(event);
            UNIT_ASSERT(std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(*event));
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 0);

            const auto commitResult = tx.Commit().ExtractValueSync();
            UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->AddCalls(), 1);
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 0);
            session.reset();
            driver.Stop(true);
        }

        Y_UNIT_TEST(RealTransactionPrecommitRecordsQueuedOffsets) {
            TTopicSdkTestSetup setup("DeliveredMessages.TransactionCallback");
            setup.Write("payload");

            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            NTable::TTableClient tables(driver);
            auto tableResult = tables.GetSession().ExtractValueSync();
            UNIT_ASSERT_C(tableResult.IsSuccess(), tableResult.GetIssues().ToString());
            auto txResult = tableResult.GetSession().BeginTransaction().ExtractValueSync();
            UNIT_ASSERT_C(txResult.IsSuccess(), txResult.GetIssues().ToString());
            auto tx = txResult.GetTransaction();
            TCapturingTransaction capturedTx(tx);
            const auto labels = MakeLabels(
                setup, setup.GetTopicPath(), setup.GetConsumerName(), "tx-pending-reader");
            auto session = client.CreateReadSession(TReadSessionSettings()
                                                        .ConsumerName(setup.GetConsumerName())
                                                        .ReaderName("tx-pending-reader")
                                                        .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
            auto commitCounter = registry->Find("ydb.topic.reader.commit.queued", labels);
            UNIT_ASSERT(commitCounter);
            auto acknowledgedCounter = registry->Find("ydb.topic.reader.commit.acknowledged", labels);
            UNIT_ASSERT(acknowledgedCounter);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto start = session->GetEvent(false);
            ConfirmStartEvent(start);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto event = session->GetEvent(
                TReadSessionGetEventSettings()
                    .Block(false)
                    .MaxEventsCount(1)
                    .Tx(static_cast<TTransactionBase&>(capturedTx)));
            UNIT_ASSERT(event);
            UNIT_ASSERT(std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(*event));
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 0);
            UNIT_ASSERT_VALUES_EQUAL(acknowledgedCounter->Value(), 0);
            auto precommit = capturedTx.TakePrecommit();
            UNIT_ASSERT(precommit);
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 0);

            auto submitted = precommit();

            UNIT_ASSERT(submitted.Initialized());
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledgedCounter->Value(), 0);
            session.reset();
            driver.Stop(true);
            UNIT_ASSERT(submitted.Wait(TDuration::Seconds(5)));
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledgedCounter->Value(), 0);
        }

        Y_UNIT_TEST(RealTransactionEventCommitIsRejectedWithoutQueueing) {
            TTopicSdkTestSetup setup("DeliveredMessages.TransactionExplicitCommit");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            NTable::TTableClient tables(driver);
            auto tableResult = tables.GetSession().ExtractValueSync();
            UNIT_ASSERT_C(tableResult.IsSuccess(), tableResult.GetIssues().ToString());
            auto txResult = tableResult.GetSession().BeginTransaction().ExtractValueSync();
            UNIT_ASSERT_C(txResult.IsSuccess(), txResult.GetIssues().ToString());
            auto tx = txResult.GetTransaction();
            const auto labels = MakeLabels(
                setup, setup.GetTopicPath(), setup.GetConsumerName(), "tx-explicit-reader");
            auto session = client.CreateReadSession(TReadSessionSettings()
                                                        .ConsumerName(setup.GetConsumerName())
                                                        .ReaderName("tx-explicit-reader")
                                                        .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
            auto commitCounter = registry->Find("ydb.topic.reader.commit.queued", labels);
            UNIT_ASSERT(commitCounter);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto start = session->GetEvent(false);
            ConfirmStartEvent(start);
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto event = session->GetEvent(
                TReadSessionGetEventSettings()
                    .Block(false)
                    .MaxEventsCount(1)
                    .Tx(static_cast<TTransactionBase&>(tx)));
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            bool rejected = false;
            try {
                data->Commit();
            } catch (...) {
                rejected = true;
            }
            UNIT_ASSERT(rejected);
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 0);

            const auto commitResult = tx.Commit().ExtractValueSync();
            UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(commitCounter->Value(), 1);
            session.reset();
            driver.Stop(true);
        }

        Y_UNIT_TEST(RealTransactionCountsUnionedRangesPerTopic) {
            TTopicSdkTestSetup setup("DeliveredMessages.TransactionUnion", TTopicSdkTestSetup::MakeServerSettings(), false);
            setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);
            setup.CreateTopic("second-topic", TEST_CONSUMER, 1);
            setup.Write(TEST_TOPIC, "first-partition-0", 0);
            setup.Write(TEST_TOPIC, "first-partition-1", 0);
            setup.Write(TEST_TOPIC, "second-partition", 1);
            setup.Write(setup.GetTopicPath("second-topic"), "other-topic-0", 0);
            setup.Write(setup.GetTopicPath("second-topic"), "other-topic-1", 0);

            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            NTable::TTableClient tables(driver);
            auto tableResult = tables.GetSession().ExtractValueSync();
            UNIT_ASSERT_C(tableResult.IsSuccess(), tableResult.GetIssues().ToString());
            auto txResult = tableResult.GetSession().BeginTransaction().ExtractValueSync();
            UNIT_ASSERT_C(txResult.IsSuccess(), txResult.GetIssues().ToString());
            auto tx = txResult.GetTransaction();

            const auto firstLabels = MakeLabels(
                setup, setup.GetTopicPath(), setup.GetConsumerName(), "tx-union-reader");
            const auto secondLabels = MakeLabels(
                setup, setup.GetTopicPath("second-topic"), setup.GetConsumerName(), "tx-union-reader");

            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(setup.GetConsumerName())
                    .ReaderName("tx-union-reader")
                    .AppendTopics(TTopicReadSettings(setup.GetTopicPath()))
                    .AppendTopics(TTopicReadSettings(setup.GetTopicPath("second-topic"))));
            auto firstCounter = registry->Find("ydb.topic.reader.commit.queued", firstLabels);
            auto secondCounter = registry->Find("ydb.topic.reader.commit.queued", secondLabels);
            UNIT_ASSERT(firstCounter);
            UNIT_ASSERT(secondCounter);
            auto settings = TReadSessionGetEventSettings()
                                .Block(false)
                                .MaxEventsCount(3)
                                .Tx(static_cast<TTransactionBase&>(tx));
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            auto waitForEvent = [&] {
                const auto remaining = deadline - TInstant::Now();
                UNIT_ASSERT_C(remaining > TDuration::Zero(), "timed out waiting for the multi-topic events");
                UNIT_ASSERT_C(session->WaitEvent().Wait(remaining), "timed out waiting for the multi-topic events");
            };

            size_t starts = 0;
            size_t dataMessages = 0;
            std::vector<TReadSessionEvent::TEvent> beforeStarts;
            while (starts < 3) {
                waitForEvent();
                auto event = session->GetEvent(settings);
                UNIT_ASSERT(event);
                if (std::holds_alternative<TReadSessionEvent::TStartPartitionSessionEvent>(*event)) {
                    ++starts;
                }
                beforeStarts.push_back(std::move(*event));
            }

            std::vector<TReadSessionEvent::TEvent> collected;
            for (auto& event : beforeStarts) {
                if (auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                    start->Confirm();
                }
                if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
                    dataMessages += data->GetMessages().size();
                }
                collected.push_back(std::move(event));
            }

            while (dataMessages < 5) {
                waitForEvent();
                auto events = session->GetEvents(settings);
                UNIT_ASSERT(!events.empty());
                for (auto& event : events) {
                    if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event)) {
                        dataMessages += data->GetMessages().size();
                    }
                    collected.push_back(std::move(event));
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(firstCounter->Value(), 0);
            UNIT_ASSERT_VALUES_EQUAL(secondCounter->Value(), 0);

            const auto commitResult = tx.Commit().ExtractValueSync();
            UNIT_ASSERT_C(commitResult.IsSuccess(), commitResult.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(firstCounter->Value(), 3);
            UNIT_ASSERT_VALUES_EQUAL(secondCounter->Value(), 2);
            session.reset();
            driver.Stop(true);
        }

        Y_UNIT_TEST(InlineHandlerCanReenterReaderWithoutQueueLock) {
            TTopicSdkTestSetup setup("DeliveredMessages.Inline");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            auto completed = NThreading::NewPromise<void>();
            std::shared_ptr<IReadSession> session;
            auto labels = MakeLabels(setup, setup.GetTopicPath(), setup.GetConsumerName(), "inline-reader");
            auto handlers = TReadSessionSettings::TEventHandlers()
                                .HandlersExecutor(std::make_shared<TInlineExecutor>())
                                .DataReceivedHandler([&](TReadSessionEvent::TDataReceivedEvent&) {
                                    session->GetEvent(false);
                                    auto counter = registry->Find("ydb.topic.reader.delivered.messages", labels);
                                    Y_ABORT_UNLESS(counter && counter->Value() == 1);
                                    completed.TrySetValue();
                                });
            session = client.CreateReadSession(TReadSessionSettings()
                                                   .ConsumerName(setup.GetConsumerName())
                                                   .ReaderName("inline-reader")
                                                   .AppendTopics(TTopicReadSettings(setup.GetTopicPath()))
                                                   .EventHandlers(handlers));
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto start = session->GetEvent(false);
            ConfirmStartEvent(start);
            Y_ABORT_UNLESS(completed.GetFuture().Wait(TDuration::Seconds(10)), "Inline reader handler deadlocked");
            session->Close(TDuration::Seconds(5));
            session.reset();
            driver.Stop(true);
        }

        Y_UNIT_TEST(CountsLogicalMessageCountAndNormalizesTopicPath) {
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto settings = TReadSessionSettings()
                                .ConsumerName("consumer")
                                .ReaderName("reader")
                                .AppendTopics(TTopicReadSettings("topic"));
            auto metrics = TReaderMetrics::Create(
                registry, "endpoint", "/Root", settings);
            UNIT_ASSERT(metrics);

            const auto labels = NMetrics::TLabels{
                {"endpoint", "endpoint"},
                {"database", "/Root"},
                {"topic", "topic"},
                {"consumer", "consumer"},
                {"reader.name", "reader"},
            };
            const auto counter = registry->Find(
                "ydb.topic.reader.delivered.messages", labels);
            UNIT_ASSERT(counter);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);

            TReadSessionEvent::TDataReceivedEvent emptyEvent(
                std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage>{},
                std::vector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>{},
                TPartitionSession::TPtr());
            metrics->RecordDelivered(emptyEvent);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);

            auto partitionSession = MakeIntrusive<TTestPartitionSession>("/Root/topic");
            TReadSessionEvent::TDataReceivedEvent::TMessageInformation messageInfo(
                0, "producer", 0, TInstant::Now(), TInstant::Now(),
                TWriteSessionMeta::TPtr(), TMessageMeta::TPtr(), 0, "group", 3);
            std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
            messages.emplace_back("payload", std::exception_ptr{}, messageInfo, partitionSession);
            std::vector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> noCompressedMessages;
            TReadSessionEvent::TDataReceivedEvent event(
                std::move(messages), std::move(noCompressedMessages), partitionSession);

            metrics->RecordDelivered(event);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 3);

            TReadSessionEvent::TDataReceivedEvent::TMessageInformation compressedInfo(
                3, "producer", 1, TInstant::Now(), TInstant::Now(),
                TWriteSessionMeta::TPtr(), TMessageMeta::TPtr(), 0, "group", 4);
            std::vector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> compressedMessages;
            compressedMessages.emplace_back(ECodec::RAW, "compressed", compressedInfo, partitionSession);
            std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> noMessages;
            TReadSessionEvent::TDataReceivedEvent compressedEvent(
                std::move(noMessages), std::move(compressedMessages), partitionSession);

            metrics->RecordDelivered(compressedEvent);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 7);

            TReadSessionEvent::TDataReceivedEvent::TMessageInformation brokenInfo(
                7, "producer", 2, TInstant::Now(), TInstant::Now(),
                TWriteSessionMeta::TPtr(), TMessageMeta::TPtr(), 0, "group", 2);
            std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> brokenMessages;
            brokenMessages.emplace_back(
                "broken", std::make_exception_ptr(std::runtime_error("decompression failed")),
                brokenInfo, partitionSession);
            std::vector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage> noBrokenCompressedMessages;
            TReadSessionEvent::TDataReceivedEvent brokenEvent(
                std::move(brokenMessages), std::move(noBrokenCompressedMessages), partitionSession);

            metrics->RecordDelivered(brokenEvent);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 9);

            auto withoutConsumerSettings = TReadSessionSettings()
                                               .WithoutConsumer()
                                               .ReaderName("without-consumer-reader")
                                               .AppendTopics(TTopicReadSettings("topic"));
            auto withoutConsumerMetrics = TReaderMetrics::Create(
                registry, "endpoint", "/Root", withoutConsumerSettings);
            UNIT_ASSERT(withoutConsumerMetrics);
            const auto withoutConsumerLabels = NMetrics::TLabels{
                {"endpoint", "endpoint"},
                {"database", "/Root"},
                {"topic", "topic"},
                {"reader.name", "without-consumer-reader"},
            };
            const auto withoutConsumerCounter = registry->Find(
                "ydb.topic.reader.delivered.messages", withoutConsumerLabels);
            UNIT_ASSERT(withoutConsumerCounter);
            auto unexpectedConsumerLabels = withoutConsumerLabels;
            unexpectedConsumerLabels.emplace("consumer", "consumer");
            UNIT_ASSERT(!registry->Find(
                "ydb.topic.reader.delivered.messages", unexpectedConsumerLabels));
            withoutConsumerMetrics->RecordDelivered(event);
            UNIT_ASSERT_VALUES_EQUAL(withoutConsumerCounter->Value(), 3);
        }

        Y_UNIT_TEST(OffsetsCollectorCollectsAdjacentAndDisjointRanges) {
            TOffsetsCollector collector;
            TReadSessionEvent::TEvent first = MakeDataEventAt("/Root/topic", 0, 3);
            TReadSessionEvent::TEvent adjacent = MakeDataEventAt("/Root/topic", 3, 2);
            TReadSessionEvent::TEvent disjoint = MakeDataEventAt("/Root/topic", 7, 1);

            collector.CollectOffsets(first);
            collector.CollectOffsets(adjacent);
            collector.CollectOffsets(disjoint);

            const auto topics = collector.GetOffsets();
            UNIT_ASSERT_VALUES_EQUAL(topics.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(topics.front().Path, "/Root/topic");
            UNIT_ASSERT_VALUES_EQUAL(topics.front().Partitions.size(), 1);
            const auto& ranges = topics.front().Partitions.front().Offsets;
            UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(ranges.front().Start, 0);
            UNIT_ASSERT_VALUES_EQUAL(ranges.front().End, 5);
            UNIT_ASSERT_VALUES_EQUAL(ranges.back().Start, 7);
            UNIT_ASSERT_VALUES_EQUAL(ranges.back().End, 8);
        }

        Y_UNIT_TEST(PreservesRelativeAndAbsoluteTopicPaths) {
            auto registry = std::make_shared<TRecordingMetricRegistry>();

            auto relativeSettings = TReadSessionSettings()
                                        .ConsumerName("consumer")
                                        .ReaderName("relative-reader")
                                        .AppendTopics(TTopicReadSettings("Root/topic"));
            auto relativeMetrics = TReaderMetrics::Create(
                registry, "endpoint", "/Root", relativeSettings);
            const auto relativeCounter = registry->Find(
                "ydb.topic.reader.delivered.messages",
                {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "Root/topic"},
                 {"consumer", "consumer"},
                 {"reader.name", "relative-reader"}});
            UNIT_ASSERT(relativeMetrics);
            UNIT_ASSERT(relativeCounter);

            auto relativeEvent = MakeDataEvent("/Root/Root/topic", 2);
            relativeMetrics->RecordDelivered(relativeEvent);
            UNIT_ASSERT_VALUES_EQUAL(relativeCounter->Value(), 2);

            auto absoluteSettings = TReadSessionSettings()
                                        .ConsumerName("consumer")
                                        .ReaderName("absolute-reader")
                                        .AppendTopics(TTopicReadSettings("/Root/topic"));
            auto absoluteMetrics = TReaderMetrics::Create(
                registry, "endpoint", "/Root", absoluteSettings);
            const auto absoluteCounter = registry->Find(
                "ydb.topic.reader.delivered.messages",
                {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                 {"consumer", "consumer"},
                 {"reader.name", "absolute-reader"}});
            UNIT_ASSERT(absoluteMetrics);
            UNIT_ASSERT(absoluteCounter);

            auto absoluteEvent = MakeDataEvent("/Root/topic", 3);
            absoluteMetrics->RecordDelivered(absoluteEvent);
            UNIT_ASSERT_VALUES_EQUAL(absoluteCounter->Value(), 3);
        }

        Y_UNIT_TEST(ExactFullPathWinsOverAnotherTopicsRelativeAlias) {
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto settings = TReadSessionSettings()
                                .ConsumerName("consumer")
                                .ReaderName("reader")
                                .AppendTopics(TTopicReadSettings("topic"))
                                .AppendTopics(TTopicReadSettings("Root/topic"));
            auto metrics = TReaderMetrics::Create(registry, "endpoint", "/Root", settings);
            UNIT_ASSERT(metrics);
            auto labels = NMetrics::TLabels{
                {"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                {"consumer", "consumer"},
                {"reader.name", "reader"}};
            const auto topicCounter = registry->Find("ydb.topic.reader.delivered.messages", labels);
            labels["topic"] = "Root/topic";
            const auto nestedCounter = registry->Find("ydb.topic.reader.delivered.messages", labels);
            UNIT_ASSERT(topicCounter);
            UNIT_ASSERT(nestedCounter);
            UNIT_ASSERT(topicCounter != nestedCounter);

            metrics->RecordDelivered(MakeDataEvent("/Root/topic", 2));
            UNIT_ASSERT_VALUES_EQUAL(topicCounter->Value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(nestedCounter->Value(), 0);
            metrics->RecordDelivered(MakeDataEvent("/Root/Root/topic", 3));
            UNIT_ASSERT_VALUES_EQUAL(topicCounter->Value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(nestedCounter->Value(), 3);
            metrics->RecordDelivered(MakeDataEvent("topic", 5));
            UNIT_ASSERT_VALUES_EQUAL(topicCounter->Value(), 7);
            UNIT_ASSERT_VALUES_EQUAL(nestedCounter->Value(), 3);
            metrics->RecordDelivered(MakeDataEvent("Root/topic", 7));
            UNIT_ASSERT_VALUES_EQUAL(topicCounter->Value(), 7);
            UNIT_ASSERT_VALUES_EQUAL(nestedCounter->Value(), 10);
            metrics->RecordDelivered(MakeDataEvent("/Root/topic/", 11));
            UNIT_ASSERT_VALUES_EQUAL(topicCounter->Value(), 18);
            UNIT_ASSERT_VALUES_EQUAL(nestedCounter->Value(), 10);
            // Both configured topics are suffixes, so this cannot be resolved.
            metrics->RecordDelivered(MakeDataEvent("/service/Root/topic", 13));
            UNIT_ASSERT_VALUES_EQUAL(topicCounter->Value(), 18);
            UNIT_ASSERT_VALUES_EQUAL(nestedCounter->Value(), 10);
            // Four counters are registered for each configured topic.
            UNIT_ASSERT_VALUES_EQUAL(registry->RegistrationAttempts(), 8);
        }

        Y_UNIT_TEST(MetricBackendFailuresDoNotEscapeOrReregisterOnDelivery) {
            auto settings = TReadSessionSettings()
                                .ConsumerName("consumer")
                                .ReaderName("reader")
                                .AppendTopics(TTopicReadSettings("topic"));
            UNIT_ASSERT(!TReaderMetrics::Create({}, "endpoint", "/Root", settings));

            for (const auto failure : {TRecordingMetricRegistry::EFailure::NullCounter,
                                       TRecordingMetricRegistry::EFailure::Registration,
                                       TRecordingMetricRegistry::EFailure::Add}) {
                auto registry = std::make_shared<TRecordingMetricRegistry>(failure);
                auto metrics = TReaderMetrics::Create(registry, "endpoint", "/Root", settings);
                UNIT_ASSERT(metrics);
                // Exactly four Reader instruments are registered.
                UNIT_ASSERT_VALUES_EQUAL(registry->RegistrationAttempts(), 4);
                metrics->RecordDelivered(MakeDataEvent("/Root/topic", 3));
                metrics->RecordDelivered(MakeDataEvent("/Root/topic", 5));
                UNIT_ASSERT_VALUES_EQUAL(registry->RegistrationAttempts(), 4);
                if (failure == TRecordingMetricRegistry::EFailure::Add) {
                    const auto counter = registry->Find("ydb.topic.reader.delivered.messages",
                                                        {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                                                         {"consumer", "consumer"},
                                                         {"reader.name", "reader"}});
                    UNIT_ASSERT(counter);
                    UNIT_ASSERT_VALUES_EQUAL(counter->AddCalls(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
                }
            }
        }

        Y_UNIT_TEST(PullCountsOnlyDataReturnedToApplication) {
            CheckPullDelivery(false, false);
        }

        Y_UNIT_TEST(PullBatchCountsOnlyDataReturnedToApplication) {
            CheckPullDelivery(true, false);
        }

        Y_UNIT_TEST(PullSettingsCountsOnlyDataReturnedToApplication) {
            CheckPullDelivery(false, true);
        }

        Y_UNIT_TEST(PullBatchSettingsCountsOnlyDataReturnedToApplication) {
            CheckPullDelivery(true, true);
        }

        Y_UNIT_TEST(CallbackCountsInsideHandlerAndWaitsForExecutor) {
            TTopicSdkTestSetup setup("DeliveredMessages.Callback");
            setup.Write("payload");

            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);

            const std::string topic = setup.GetTopicPath();
            const std::string consumer = setup.GetConsumerName();
            const std::string reader = "callback-reader";
            auto executor = std::make_shared<TManualExecutor>();
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(consumer)
                    .ReaderName(reader)
                    .AppendTopics(TTopicReadSettings(topic))
                    .EventHandlers(TReadSessionSettings::TEventHandlers()
                                       .DataReceivedHandler([registry, setupEndpoint = setup.GetEndpoint(),
                                                             database = setup.GetDatabase(), topic, consumer, reader](
                                                                TReadSessionEvent::TDataReceivedEvent& event) {
                                           const auto counter = registry->Find(
                                               "ydb.topic.reader.delivered.messages",
                                               {{"endpoint", setupEndpoint}, {"database", database}, {"topic", topic},
                                                {"consumer", consumer},
                                                {"reader.name", reader}});
                                           UNIT_ASSERT(counter);
                                           UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);
                                           UNIT_ASSERT_VALUES_EQUAL(event.GetMessages().size(), 1);
                                       })
                                       .HandlersExecutor(executor)));

            const auto counter = registry->Find(
                "ydb.topic.reader.delivered.messages", MakeLabels(setup, topic, consumer, reader));
            UNIT_ASSERT(counter);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto firstEvent = session->GetEvent(false);
            ConfirmStartEvent(firstEvent);
            UNIT_ASSERT(executor->WaitForTask());
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);

            executor->RunOne();
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);

            session->Close(TDuration::Seconds(5));
            session.reset();
            executor->Stop();
            driver.Stop(true);
        }

        Y_UNIT_TEST(CommonHandlerCountsInsideHandlerAndWaitsForExecutor) {
            CheckCommonHandlerDelivery(false);
        }

        Y_UNIT_TEST(DataHandlerTakesPriorityOverCommonHandlerWithoutDoubleCounting) {
            CheckCommonHandlerDelivery(true);
        }

        Y_UNIT_TEST(QueuedCallbackKeepsMetricsAliveAfterSessionDestruction) {
            CheckCommonHandlerDelivery(false, true);
        }
    } // Y_UNIT_TEST_SUITE(TReaderMetricsTest)

} // namespace NYdb::inline Dev::NTopic::NTests
