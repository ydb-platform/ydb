#include "ut_utils/topic_sdk_test_setup.h"
#include "reader_metrics_test_utils.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/executor/executor.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/direct_reader.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session.h>

#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <future>
#include <functional>
#include <limits>
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


        TReadSessionEvent::TDataReceivedEvent MakeDataEvent(
            const std::string& topicPath, std::uint64_t logicalMessageCount)
        {
            auto partitionSession = MakeIntrusive<TTestPartitionSession>(topicPath);
            TReadSessionEvent::TDataReceivedEvent::TMessageInformation messageInfo(
                0, "producer", 0, TInstant::Now(), TInstant::Now(),
                TWriteSessionMeta::TPtr(), TMessageMeta::TPtr(), 0, "group", logicalMessageCount);
            std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> messages;
            messages.emplace_back("payload", std::exception_ptr{}, std::move(messageInfo), partitionSession);
            return TReadSessionEvent::TDataReceivedEvent(
                std::move(messages),
                std::vector<TReadSessionEvent::TDataReceivedEvent::TCompressedMessage>{},
                partitionSession);
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

    } // anonymous namespace

    class TReaderMetricsTestPeer {
    public:
        using TResponse = Ydb::Topic::StreamReadMessage::ReadResponse;
        using TDirectResponse = Ydb::Topic::StreamDirectReadMessage::DirectReadResponse;

        class TStoppedContext final: public NYdbGrpc::IQueueClientContext {
        public:
            NYdbGrpc::IQueueClientContextPtr CreateContext() override {
                return {};
            }
            grpc::CompletionQueue* CompletionQueue() override {
                return nullptr;
            }
            bool IsCancelled() const override {
                return true;
            }
            bool Cancel() override {
                return false;
            }
            void SubscribeCancel(std::function<void()> callback) override {
                callback();
            }
        };

        class TProcessor final: public IProcessor<false> {
        public:
            void Cancel() override {
                Cancelled = true;
            }
            void ReadInitialMetadata(std::unordered_multimap<std::string, std::string>*, TReadCallback) override {
            }
            void Finish(TReadCallback) override {
            }
            void AddFinishedCallback(TReadCallback) override {
            }
            void Write(TClientMessage<false>&& request, TWriteCallback = {}) override {
                if (request.has_read_request()) {
                    ReadRequestSizes.push_back(request.read_request().bytes_size());
                }
                if (request.has_commit_offset_request()) {
                    for (const auto& commit : request.commit_offset_request().commit_offsets()) {
                        for (const auto& range : commit.offsets()) {
                            CommitRanges.emplace_back(range.start(), range.end());
                        }
                    }
                }
                if (request.has_direct_read_ack()) {
                    const auto& acknowledgement = request.direct_read_ack();
                    DirectReadAcks.emplace_back(
                        acknowledgement.partition_session_id(), acknowledgement.direct_read_id());
                }
            }
            void Read(TServerMessage<false>* response, TReadCallback callback) override {
                Destination = response;
                Callback = std::move(callback);
                if (OnRead) {
                    OnRead();
                }
            }
            void Reply(TResponse response, NYdbGrpc::TGrpcStatus status = {},
                       Ydb::StatusIds::StatusCode serverStatus = Ydb::StatusIds::SUCCESS) {
                TServerMessage<false> message;
                message.set_status(serverStatus);
                *message.mutable_read_response() = std::move(response);
                ReplyMessage(std::move(message), std::move(status));
            }

            void ReplyCommitAcknowledgement(
                ui64 partitionSessionId, i64 committedOffset,
                Ydb::StatusIds::StatusCode serverStatus = Ydb::StatusIds::SUCCESS) {
                TServerMessage<false> message;
                message.set_status(serverStatus);
                auto* partition = message.mutable_commit_offset_response()->add_partitions_committed_offsets();
                partition->set_partition_session_id(partitionSessionId);
                partition->set_committed_offset(committedOffset);
                ReplyMessage(std::move(message));
            }
            void ReplyMessage(TServerMessage<false> message, NYdbGrpc::TGrpcStatus status = {}) {
                UNIT_ASSERT(Callback);
                *Destination = std::move(message);
                auto callback = std::exchange(Callback, {});
                callback(std::move(status));
            }
            void Clear() {
                Callback = {};
                OnRead = {};
            }

            bool Cancelled = false;
            std::vector<i64> ReadRequestSizes;
            std::vector<std::pair<ui64, ui64>> CommitRanges;
            std::vector<std::pair<ui64, ui64>> DirectReadAcks;
            std::function<void()> OnRead;
            TServerMessage<false>* Destination = nullptr;
            TReadCallback Callback;
        };

        TReaderMetricsTestPeer(
            std::vector<std::string> topics = {"topic"},
            bool decompress = false,
            std::shared_ptr<TRecordingMetricRegistry> registry = {},
            bool directRead = false)
            : Registry(registry ? std::move(registry) : std::make_shared<TRecordingMetricRegistry>())
            , Executor(std::make_shared<TManualExecutor>())
            , Processor(MakeIntrusive<TProcessor>())
        {
            auto counters = MakeIntrusive<TReaderCounters>();
            MakeCountersNotNull(*counters);
            auto clientContext = std::make_shared<TStoppedContext>();
            Settings.ConsumerName("consumer").ReaderName("wire-reader").Counters(counters).Decompress(decompress).DecompressionExecutor(Executor).DirectRead(directRead);
            Settings.RetryPolicy(IRetryPolicy::GetDefaultPolicy());
            Settings.EventHandlers_.HandlersExecutor(Executor);
            for (const auto& topic : topics) {
                Settings.AppendTopics(TTopicReadSettings(topic));
            }
            Metrics = TReaderMetrics::Create(Registry, "endpoint", "/Root", Settings);
            Queue = std::make_shared<TReadSessionEventsQueue<false>>(Settings, Metrics);
            Reader = std::make_shared<TSingleClusterReadSessionImpl<false>>(
                Settings, "/Root", "wire-session", "", TLog{}, nullptr, Queue,
                clientContext, 1, 1,
                TSingleClusterReadSessionImpl<false>::TScheduleCallbackFunc{}, nullptr, Metrics);
            Reader->SetSelfContext(Reader);
            Context = Reader->SelfContext;
            Queue->SetCallbackContext(Context);
            Reader->Processor = Processor;
            Reader->ServerMessage = std::make_shared<TServerMessage<false>>();
            Reader->ConnectionGeneration = 1;
            Reader->ReadSessionId = "wire-session";
            if (directRead) {
                Reader->DirectReadSessionManager.emplace(
                    "wire-session",
                    Settings,
                    std::make_shared<TDirectReadSessionControlCallbacks>(Context),
                    clientContext,
                    nullptr,
                    TLog{});
            }
            Arm();
        }

        ~TReaderMetricsTestPeer() {
            Processor->Clear();
            DestroyReaderForTests();
        }

        void DestroyReaderForTests() {
            if (Reader) {
                Reader->Abort();
                Reader->ClearAllPartitionStreamEvents();
                Queue->ClearAllEvents();
                Context->Cancel();
                Reader.reset();
                Queue.reset();
            }
        }

        void AddPartition(ui64 id, const std::string& topic = "topic", ui64 committed = 0) {
            StartPartitionFromServer(id, topic, committed);
            ConfirmStartPartition();
        }

        void CommitRangeForTests(ui64 id, ui64 startOffset, ui64 endOffset) {
            Reader->PartitionStreams.at(id)->Commit(startOffset, endOffset);
        }

        void ReplyCommitAcknowledgement(
            ui64 partitionSessionId, i64 committedOffset,
            Ydb::StatusIds::StatusCode serverStatus = Ydb::StatusIds::SUCCESS) {
            Processor->ReplyCommitAcknowledgement(partitionSessionId, committedOffset, serverStatus);
        }

        void StartPartitionFromServer(
            ui64 id,
            const std::string& topic = "topic",
            std::optional<ui64> committedOffset = std::nullopt)
        {
            TServerMessage<false> message;
            message.set_status(Ydb::StatusIds::SUCCESS);
            auto* request = message.mutable_start_partition_session_request();
            request->mutable_partition_session()->set_partition_session_id(id);
            request->mutable_partition_session()->set_partition_id(id);
            request->mutable_partition_session()->set_path(topic);
            if (committedOffset) {
                request->set_committed_offset(*committedOffset);
            }
            request->mutable_partition_location()->set_node_id(1);
            request->mutable_partition_location()->set_generation(1);
            Processor->ReplyMessage(std::move(message));
        }

        void ConfirmStartPartition() {
            auto event = Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event);
            UNIT_ASSERT(start);
            start->Confirm();
        }

        void StopPartitionFromServer(ui64 id, bool graceful, std::optional<i64> lastDirectReadId = std::nullopt) {
            TServerMessage<false> message;
            message.set_status(Ydb::StatusIds::SUCCESS);
            auto* request = message.mutable_stop_partition_session_request();
            request->set_partition_session_id(id);
            request->set_graceful(graceful);
            if (lastDirectReadId) {
                request->set_last_direct_read_id(*lastDirectReadId);
            }
            Processor->ReplyMessage(std::move(message));
        }

        static TReadSessionEvent::TEndPartitionSessionEvent BlockUntilParentEnd(
            IReadSession& session, const TPartitionSession::TPtr& child)
        {
            auto& readSession = dynamic_cast<TReadSession&>(session);
            auto reader = GetReadSessionOwner(readSession.CbContext);
            UNIT_ASSERT(reader);
            UNIT_ASSERT(child->GetPartitionSessionId() > 0);
            const auto parentId = child->GetPartitionSessionId() - 1;
            const auto parentPartition = child->GetPartitionId() + 1;
            auto parent = MakeIntrusive<TPartitionStreamImpl<false>>(
                parentId, child->GetTopicPath(), session.GetSessionId(), static_cast<i64>(parentPartition),
                static_cast<i64>(parentId), 0, std::nullopt, readSession.CbContext);
            reader->RegisterParentPartition(child->GetPartitionId(), parentPartition, parentId);
            return {parent, {}, {static_cast<ui32>(child->GetPartitionId())}};
        }

        static TIntrusivePtr<TProcessor> ReplaceReadProcessor(IReadSession& session) {
            auto& readSession = dynamic_cast<TReadSession&>(session);
            auto reader = GetReadSessionOwner(readSession.CbContext);
            UNIT_ASSERT(reader);
            auto processor = MakeIntrusive<TProcessor>();
            TDeferredActions<false> deferred;
            {
                std::lock_guard guard(reader->Lock);
                // Retire the real pending read, then install a controlled transport.
                // ReadFromProcessorImpl creates the same callback used by gRPC.
                ++reader->ConnectionGeneration;
                reader->Processor->Cancel();
                reader->Processor = processor;
                reader->ServerMessage = std::make_shared<TServerMessage<false>>();
                reader->ReadFromProcessorImpl(deferred);
            }
            return processor;
        }

        void AbortReaderForTests() {
            Reader->Abort();
        }

        void Arm() {
            TDeferredActions<false> deferred;
            std::lock_guard guard(Reader->Lock);
            Reader->ReadFromProcessorImpl(deferred);
        }

        void Stale() {
            ++Reader->ConnectionGeneration;
        }

        void Closing() {
            Reader->Closing = true;
        }

        void Aborting() {
            Reader->Aborting = true;
        }

        std::shared_ptr<TRecordingCounter> Counter(const std::string& suffix, const std::string& topic = "topic") {
            auto result = Registry->Find(
                "ydb.topic.reader." + suffix,
                {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", topic},
                 {"consumer", "consumer"},
                 {"reader.name", "wire-reader"}});
            UNIT_ASSERT(result);
            return result;
        }

        static void AddData(TResponse& response, ui64 id, ui64 offset = 0, const std::string& payload = "payload",
                            int codec = Ydb::Topic::CODEC_RAW) {
            auto* partition = response.add_partition_data();
            partition->set_partition_session_id(id);
            auto* batch = partition->add_batches();
            batch->set_codec(codec);
            auto* message = batch->add_message_data();
            message->set_offset(offset);
            message->set_data(payload);
            message->set_uncompressed_size(payload.size());
        }

        void ReconnectForTests() {
            Reader->Reconnect(TPlainStatus{});
        }
        void ReplyDirect(TDirectResponse response) {
            auto responses = std::make_shared<TLockFreeQueue<TDirectResponse>>();
            responses->Enqueue(std::move(response));
            TDirectReadSessionControlCallbacks callbacks(Context);
            callbacks.OnDirectReadDone(std::move(responses));
        }

        TReadSessionSettings Settings;
        std::shared_ptr<TRecordingMetricRegistry> Registry;
        std::shared_ptr<TManualExecutor> Executor;
        TIntrusivePtr<TProcessor> Processor;
        std::shared_ptr<TReaderMetrics> Metrics;
        std::shared_ptr<TReadSessionEventsQueue<false>> Queue;
        TSingleClusterReadSessionImpl<false>::TPtr Reader;
        TCallbackContextPtr<false> Context;
    };

    Y_UNIT_TEST_SUITE(TReceivedCountersMetricsTest) {
        Y_UNIT_TEST(EventQueueKeepsHandlerSettingsAlive) {
            auto calls = std::make_shared<size_t>(0);
            const std::weak_ptr<size_t> lifetime = calls;
            TReadSessionSettings settings;
            settings.EventHandlers_.HandlersExecutor(std::make_shared<TInlineExecutor>());
            settings.EventHandlers_.SessionClosedHandler([calls](const TSessionClosedEvent&) {
                ++*calls;
            });
            auto queue = std::make_shared<TReadSessionEventsQueue<false>>(settings);

            calls.reset();
            settings = TReadSessionSettings();
            UNIT_ASSERT(!lifetime.expired());
            {
                TDeferredActions<false> deferred;
                UNIT_ASSERT(queue->Close(TSessionClosedEvent(EStatus::SUCCESS, {}), deferred));
            }
            UNIT_ASSERT_VALUES_EQUAL(*lifetime.lock(), 1);
            queue.reset();
            UNIT_ASSERT(lifetime.expired());
        }

        Y_UNIT_TEST(WireMessagesAccumulatePerTopicAcrossPartitions) {
            TReaderMetricsTestPeer peer({"first", "second"});
            peer.AddPartition(1, "first");
            peer.AddPartition(2, "second");
            peer.AddPartition(3, "first");
            TReaderMetricsTestPeer::TResponse response;
            response.set_bytes_size(101);
            for (ui64 id : {1, 2, 3}) {
                TReaderMetricsTestPeer::AddData(response, id);
            }
            UNIT_ASSERT(response.ByteSizeLong() != 101);
            UNIT_ASSERT_VALUES_EQUAL(response.partition_data(0).ByteSizeLong(), response.partition_data(2).ByteSizeLong());
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages", "first")->Value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages", "second")->Value(), 1);
        }

        Y_UNIT_TEST(WireUnknownPartitionDoesNotCountTail) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1);
            peer.AddPartition(3);
            TReaderMetricsTestPeer::TResponse response;
            response.set_bytes_size(101);
            for (ui64 id : {1, 2, 3}) {
                TReaderMetricsTestPeer::AddData(response, id);
            }
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 1);
            UNIT_ASSERT(peer.Processor->Cancelled);
        }

        Y_UNIT_TEST(MessageCountsIgnoreResponseByteSizeAndEmptyResponses) {
            for (i64 bytes : {-1, 0, 101}) {
                TReaderMetricsTestPeer peer;
                peer.AddPartition(1);
                TReaderMetricsTestPeer::TResponse response;
                response.set_bytes_size(bytes);
                TReaderMetricsTestPeer::AddData(response, 1);
                peer.Processor->Reply(std::move(response));
                UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 1);
            }
            for (bool multiple : {false, true}) {
                TReaderMetricsTestPeer peer(multiple ? std::vector<std::string>{"topic", "other"} : std::vector<std::string>{"topic"});
                TReaderMetricsTestPeer::TResponse response;
                response.set_bytes_size(101);
                peer.Processor->Reply(std::move(response));
                UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 0);
            }
        }

        Y_UNIT_TEST(EmptyKnownPartitionDoesNotCountBeforeProtocolError) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1);
            TReaderMetricsTestPeer::TResponse response;
            response.set_bytes_size(71);
            response.add_partition_data()->set_partition_session_id(1);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 0);
            UNIT_ASSERT(peer.Processor->Cancelled);
        }

        Y_UNIT_TEST(WireGuardsIgnoreStaleClosingAbortingAndErrors) {
            for (int mode = 0; mode < 5; ++mode) {
                TReaderMetricsTestPeer peer;
                peer.AddPartition(1);
                TReaderMetricsTestPeer::TResponse response;
                response.set_bytes_size(101);
                TReaderMetricsTestPeer::AddData(response, 1);
                if (mode == 0) {
                    peer.Stale();
                }
                if (mode == 1) {
                    peer.Closing();
                }
                if (mode == 2) {
                    peer.Aborting();
                }
                peer.Processor->Reply(std::move(response),
                                      mode == 3 ? NYdbGrpc::TGrpcStatus(grpc::StatusCode::UNAVAILABLE, "test") : NYdbGrpc::TGrpcStatus{},
                                      mode == 4 ? Ydb::StatusIds::OVERLOADED : Ydb::StatusIds::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 0);
            }
        }

        Y_UNIT_TEST(DirectControlEmptyAndDataResponsesCountExactlyOnce) {
            for (int mode = 0; mode < 5; ++mode) {
                TReaderMetricsTestPeer peer;
                peer.AddPartition(1);
                TReaderMetricsTestPeer::TDirectResponse response;
                response.set_partition_session_id(mode == 2 ? 2 : 1);
                response.set_direct_read_id(1);
                response.set_bytes_size(101);
                response.mutable_partition_data()->set_partition_session_id(response.partition_session_id());
                if (mode == 1) {
                    TReaderMetricsTestPeer::TResponse data;
                    TReaderMetricsTestPeer::AddData(data, 1);
                    *response.mutable_partition_data() = data.partition_data(0);
                }
                if (mode == 3) {
                    peer.Closing();
                }
                if (mode == 4) {
                    peer.Aborting();
                }
                peer.ReplyDirect(std::move(response));
                UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), mode == 1 ? 1 : 0);
            }
        }

        Y_UNIT_TEST(ReceivedBackendCanCancelCallbackContextOnBothPaths) {
            // FORK_SUBTESTS plus bounded runner timeout makes a lock regression a
            // failed isolated test, not a hang of the entire test process group.
            for (bool direct : {false, true}) {
                TReaderMetricsTestPeer peer;
                peer.AddPartition(1);
                bool called = false;
                peer.Counter("received.messages")->OnAdd = [&] {
                    peer.Context->Cancel();
                    called = true;
                };
                if (direct) {
                    TReaderMetricsTestPeer::TDirectResponse response;
                    response.set_partition_session_id(1);
                    response.set_direct_read_id(1);
                    TReaderMetricsTestPeer::TResponse data;
                    TReaderMetricsTestPeer::AddData(data, 1);
                    *response.mutable_partition_data() = data.partition_data(0);
                    response.set_bytes_size(101);
                    peer.ReplyDirect(std::move(response));
                } else {
                    TReaderMetricsTestPeer::TResponse response;
                    response.set_bytes_size(101);
                    TReaderMetricsTestPeer::AddData(response, 1);
                    peer.Processor->Reply(std::move(response));
                }
                UNIT_ASSERT(called);
                UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 1);
            }
        }

        Y_UNIT_TEST(ExportPrecedesImmediateNextReadWithoutGlobalOrderingPromise) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1);
            bool nextRead = false;
            peer.Processor->OnRead = [&] {
                UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 1);
                nextRead = true;
            };
            TReaderMetricsTestPeer::TResponse response;
            response.set_bytes_size(101);
            TReaderMetricsTestPeer::AddData(response, 1);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(nextRead);
        }

        Y_UNIT_TEST(RealReaderCanCloseFromReceivedCounterWithoutWaitingForDeferredTasks) {
            TTopicSdkTestSetup setup("ReceivedCounters.ReentrantClose");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            auto session = client.CreateReadSession(TReadSessionSettings()
                                                        .ConsumerName(setup.GetConsumerName())
                                                        .ReaderName("close-reader")
                                                        .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
            auto counter = registry->Find("ydb.topic.reader.received.messages",
                                          MakeLabels(setup, setup.GetTopicPath(), setup.GetConsumerName(), "close-reader"));
            UNIT_ASSERT(counter);
            auto completed = NThreading::NewPromise<bool>();
            counter->OnAdd = [weak = std::weak_ptr<IReadSession>(session), completed]() mutable {
                auto reader = weak.lock();
                completed.TrySetValue(reader && reader->Close(TDuration::Seconds(30)));
            };
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto event = session->GetEvent(false);
            UNIT_ASSERT(event);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event);
            UNIT_ASSERT(start);
            start->Confirm();
            // FORK_SUBTESTS isolates the watchdog abort from other tests. Unwinding
            // here would block while destroying the same reader that is stuck in Close.
            Y_ABORT_UNLESS(completed.GetFuture().Wait(TDuration::Seconds(5)),
                           "Received counter Close waited for work deferred until Add returns");
            UNIT_ASSERT(completed.GetFuture().GetValueSync());
            session.reset();
            driver.Stop(true);
        }

        Y_UNIT_TEST(RealReaderCanCloseFromDeliveredCounterWithoutJoiningItsOwnTask) {
            for (bool inlineDecompression : {false, true}) {
                TTopicSdkTestSetup setup("DeliveredCounters.ReentrantClose");
                setup.Write("payload");
                auto registry = std::make_shared<TRecordingMetricRegistry>();
                auto config = setup.MakeDriverConfig();
                config.SetMetricRegistry(registry);
                TDriver driver(std::move(config));
                TTopicClient client(driver);

                auto inlineExecutor = std::make_shared<TInlineExecutor>();
                auto decompressionExecutor = inlineDecompression
                                                 ? IExecutor::TPtr(inlineExecutor)
                                                 : CreateThreadPoolExecutor(1);
                TReadSessionSettings::TEventHandlers handlers;
                auto handlerCompleted = NThreading::NewPromise<void>();
                handlers.DataReceivedHandler([handlerCompleted](TReadSessionEvent::TDataReceivedEvent&) mutable {
                    handlerCompleted.TrySetValue();
                });
                handlers.HandlersExecutor(inlineExecutor);

                const std::string readerName = inlineDecompression ? "delivered-inline" : "delivered-async";
                auto session = client.CreateReadSession(
                    TReadSessionSettings()
                        .ConsumerName(setup.GetConsumerName())
                        .ReaderName(readerName)
                        .AppendTopics(TTopicReadSettings(setup.GetTopicPath()))
                        .DecompressionExecutor(decompressionExecutor)
                        .EventHandlers(std::move(handlers)));
                const auto delivered = registry->Find(
                    "ydb.topic.reader.delivered.messages",
                    MakeLabels(setup, setup.GetTopicPath(), setup.GetConsumerName(), readerName));
                UNIT_ASSERT(delivered);

                auto completed = NThreading::NewPromise<bool>();
                delivered->OnAdd = [weak = std::weak_ptr<IReadSession>(session), completed]() mutable {
                    auto reader = weak.lock();
                    completed.TrySetValue(reader && reader->Close(TDuration::Seconds(30)));
                };

                UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
                auto event = session->GetEvent(false);
                UNIT_ASSERT(event);
                auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event);
                UNIT_ASSERT(start);
                start->Confirm();

                Y_ABORT_UNLESS(completed.GetFuture().Wait(TDuration::Seconds(5)),
                               "Delivered counter Close waited for its own decompression task");
                Y_ABORT_UNLESS(handlerCompleted.GetFuture().Wait(TDuration::Seconds(5)),
                               "Delivered counter handler did not run");
                UNIT_ASSERT(completed.GetFuture().GetValueSync());

                session.reset();
                decompressionExecutor->Stop();
                driver.Stop(true);
            }
        }

        Y_UNIT_TEST(TransportErrorDeliversReadyPrefixOutsideContextBorrow) {
            for (const bool closeInBackend : {false, true}) {
                TTopicSdkTestSetup setup("DeliveredCounters.ErrorReadyPrefix");
                setup.Write("payload");
                auto registry = std::make_shared<TRecordingMetricRegistry>();
                auto config = setup.MakeDriverConfig();
                config.SetMetricRegistry(registry);
                TDriver driver(std::move(config));
                TTopicClient client(driver);
                auto decompression = std::make_shared<TManualExecutor>();
                std::atomic<size_t> handled = 0;
                TReadSessionSettings::TEventHandlers handlers;
                handlers.HandlersExecutor(std::make_shared<TInlineExecutor>());
                handlers.DataReceivedHandler([&handled](TReadSessionEvent::TDataReceivedEvent& event) {
                    handled.fetch_add(event.GetMessagesCount());
                });
                auto session = client.CreateReadSession(TReadSessionSettings()
                    .ConsumerName(setup.GetConsumerName())
                    .ReaderName("error-ready-prefix")
                    .AppendTopics(TTopicReadSettings(setup.GetTopicPath()))
                    .DecompressionExecutor(decompression)
                    .EventHandlers(std::move(handlers)));
                const auto delivered = registry->Find("ydb.topic.reader.delivered.messages",
                    MakeLabels(setup, setup.GetTopicPath(), setup.GetConsumerName(), "error-ready-prefix"));
                UNIT_ASSERT(delivered);
                auto closed = NThreading::NewPromise<bool>();
                if (closeInBackend) {
                    delivered->OnAdd = [weak = std::weak_ptr<IReadSession>(session), closed]() mutable {
                        auto reader = weak.lock();
                        closed.TrySetValue(reader && reader->Close(TDuration::Seconds(1)));
                    };
                }

                UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
                auto startEvent = session->GetEvent(false);
                UNIT_ASSERT(startEvent);
                auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*startEvent);
                UNIT_ASSERT(start);
                auto* partition = dynamic_cast<TPartitionStreamImpl<false>*>(start->GetPartitionSession().Get());
                UNIT_ASSERT(partition);
                start->Confirm();
                UNIT_ASSERT(decompression->WaitForTask());
                auto processor = TReaderMetricsTestPeer::ReplaceReadProcessor(*session);

                // SignalReadyEvents takes this mutex after the worker publishes
                // Ready. Holding it makes the Ready-before-signal window stable;
                // reconnect's PushEvent(Closed) can still consume that ready prefix.
                std::unique_lock signalBarrier(partition->GetLock());
                UNIT_ASSERT(partition->HasEvents());
                const auto& rawEvent = partition->TopEvent();
                UNIT_ASSERT(rawEvent.IsDataEvent());
                auto decompressed = NThreading::NewPromise<void>();
                std::thread worker([decompression, decompressed]() mutable {
                    decompression->RunOne();
                    decompressed.TrySetValue();
                });
                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                while (!rawEvent.IsReady() && std::chrono::steady_clock::now() < deadline) {
                    std::this_thread::yield();
                }
                Y_ABORT_UNLESS(rawEvent.IsReady(), "Decompression did not reach the Ready barrier");
                UNIT_ASSERT_VALUES_EQUAL(delivered->Value(), 0);

                auto replied = NThreading::NewPromise<void>();
                std::thread transport([processor, replied]() mutable {
                    processor->Reply({}, NYdbGrpc::TGrpcStatus(grpc::StatusCode::UNAVAILABLE, "test transport error"));
                    replied.TrySetValue();
                });
                // A failing implementation terminates only this forked subtest;
                // never join a thread that could be waiting for its own borrow.
                Y_ABORT_UNLESS(replied.GetFuture().Wait(TDuration::Seconds(5)),
                    "Error reconnect held the context borrow across inline backend Close");
                transport.join();
                UNIT_ASSERT_VALUES_EQUAL(delivered->Value(), 1);
                UNIT_ASSERT_VALUES_EQUAL(handled.load(), 1);
                if (closeInBackend) {
                    UNIT_ASSERT(closed.GetFuture().HasValue());
                    UNIT_ASSERT(closed.GetFuture().GetValueSync());
                }
                signalBarrier.unlock();
                Y_ABORT_UNLESS(decompressed.GetFuture().Wait(TDuration::Seconds(5)),
                    "Decompression did not finish after releasing the Ready barrier");
                worker.join();
                UNIT_ASSERT_VALUES_EQUAL(delivered->Value(), 1);
                UNIT_ASSERT_VALUES_EQUAL(handled.load(), 1);
                session->Close(TDuration::Seconds(1));
                processor->Clear();
                session.reset();
                decompression->Stop();
                driver.Stop(true);
            }
        }

        Y_UNIT_TEST(ParentEndConfirmationCanDeliverChildAndCloseReaderInline) {
            for (bool commonHandler : {false, true}) {
                TTopicSdkTestSetup setup("DeliveredCounters.ParentEnd");
                setup.Write("payload");
                auto registry = std::make_shared<TRecordingMetricRegistry>();
                auto config = setup.MakeDriverConfig();
                config.SetMetricRegistry(registry);
                TDriver driver(std::move(config));
                TTopicClient client(driver);
                auto decompression = std::make_shared<TManualExecutor>();
                auto inlineExecutor = std::make_shared<TInlineExecutor>();
                auto started = NThreading::NewPromise<TReadSessionEvent::TStartPartitionSessionEvent>();
                auto handled = NThreading::NewPromise<void>();
                TReadSessionSettings::TEventHandlers handlers;
                handlers.StartPartitionSessionHandler([started](TReadSessionEvent::TStartPartitionSessionEvent& event) mutable {
                    started.TrySetValue(event);
                });
                handlers.HandlersExecutor(inlineExecutor);
                if (commonHandler) {
                    handlers.CommonHandler([handled](TReadSessionEvent::TEvent& event) mutable {
                        if (std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(event)) {
                            handled.TrySetValue();
                        }
                    });
                } else {
                    handlers.DataReceivedHandler([handled](TReadSessionEvent::TDataReceivedEvent&) mutable {
                        handled.TrySetValue();
                    });
                }

                auto session = client.CreateReadSession(TReadSessionSettings()
                                                            .ConsumerName(setup.GetConsumerName())
                                                            .ReaderName("parent-end")
                                                            .AppendTopics(TTopicReadSettings(setup.GetTopicPath()))
                                                            .DecompressionExecutor(decompression)
                                                            .EventHandlers(std::move(handlers)));
                auto delivered = registry->Find("ydb.topic.reader.delivered.messages",
                                                MakeLabels(setup, setup.GetTopicPath(), setup.GetConsumerName(), "parent-end"));
                UNIT_ASSERT(delivered);
                auto closed = NThreading::NewPromise<bool>();
                delivered->OnAdd = [weak = std::weak_ptr<IReadSession>(session), closed]() mutable {
                    auto reader = weak.lock();
                    closed.TrySetValue(reader && reader->Close(TDuration::MilliSeconds(1)));
                };
                UNIT_ASSERT(started.GetFuture().Wait(TDuration::Seconds(5)));
                auto start = started.GetFuture().GetValueSync();
                auto parentEnd = TReaderMetricsTestPeer::BlockUntilParentEnd(*session, start.GetPartitionSession());
                start.Confirm();
                UNIT_ASSERT(decompression->WaitForTask());
                decompression->RunOne();
                UNIT_ASSERT_VALUES_EQUAL(delivered->Value(), 0);
                UNIT_ASSERT(!handled.GetFuture().HasValue());

                auto completed = NThreading::NewPromise<void>();
                std::thread confirm([parentEnd = std::move(parentEnd), completed]() mutable {
                    parentEnd.Confirm();
                    completed.TrySetValue();
                });
                Y_ABORT_UNLESS(completed.GetFuture().Wait(TDuration::Seconds(5)),
                               "Parent end confirmation held the reader context across inline Close");
                confirm.join();
                UNIT_ASSERT(closed.GetFuture().HasValue());
                UNIT_ASSERT(closed.GetFuture().GetValueSync());
                UNIT_ASSERT(handled.GetFuture().HasValue());
                UNIT_ASSERT_VALUES_EQUAL(delivered->Value(), 1);
                session.reset();
                decompression->Stop();
                driver.Stop(true);
            }
        }

        Y_UNIT_TEST(EachInstrumentCanFailIndependentlyAtRegistrationAndAdd) {
            const std::vector<std::string> names = {"delivered.messages", "received.messages", "commit.acknowledged", "commit.queued"};
            for (const auto& failed : names) {
                for (int mode = 0; mode < 3; ++mode) {
                    const std::string full = "ydb.topic.reader." + failed;
                    using EFailure = TRecordingMetricRegistry::EFailure;
                    const auto failure = mode == 0 ? EFailure::Add
                        : mode == 1 ? EFailure::Registration : EFailure::NullCounter;
                    auto registry = std::make_shared<TRecordingMetricRegistry>(failure, full);
                    auto settings = TReadSessionSettings().WithoutConsumer().ReaderName("failure-reader").AppendTopics(TTopicReadSettings("topic"));
                    auto metrics = TReaderMetrics::Create(registry, "endpoint", "/Root", settings);
                    UNIT_ASSERT(metrics);
                    metrics->RecordReceived({metrics, metrics->ResolveTopic("topic"), 2});
                    metrics->RecordDelivered(MakeDataEvent("topic", 2));
                    metrics->RecordCommitQueued(metrics->ResolveTopic("topic"), 2);
                    metrics->RecordCommitAcknowledged(metrics->ResolveTopic("topic"), 2);
                    for (const auto& name : names) {
                        auto counter = registry->Find("ydb.topic.reader." + name, {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                                                                                   {"reader.name", "failure-reader"}});
                        if (name == failed && mode != 0) {
                            UNIT_ASSERT(!counter);
                        } else {
                            UNIT_ASSERT(counter);
                            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), name == failed ? 0 : 2);
                        }
                    }
                }
            }
        }

        Y_UNIT_TEST(WireKafkaCountsWholeBatchBeforeCommittedPrefixFilteringAndOnRepeat) {
            NKafka::TKafkaRecordBatch batch;
            batch.BaseOffset = 0;
            batch.BaseSequence = 0;
            batch.LastOffsetDelta = 2;
            for (int i = 0; i < 3; ++i) {
                NKafka::TKafkaRecord record;
                record.OffsetDelta = i;
                record.SetValue("payload");
                batch.Records.push_back(std::move(record));
            }
            const TString encoded = NKafka::WriteKafkaRecordBatch(batch);
            for (bool decompress : {false, true}) {
                TReaderMetricsTestPeer peer({"topic"}, decompress);
                peer.AddPartition(1, "topic", 1);
                for (int attempt = 1; attempt <= 2; ++attempt) {
                    TReaderMetricsTestPeer::TResponse response;
                    response.set_bytes_size(101);
                    TReaderMetricsTestPeer::AddData(response, 1, 0,
                                                    std::string(encoded.data(), encoded.size()), Ydb::Topic::CODEC_KAFKA_BATCH);
                    peer.Processor->Reply(std::move(response));
                    UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 3 * attempt);
                    if (decompress) {
                        UNIT_ASSERT(peer.Executor->WaitForTask());
                        peer.Executor->RunOne();
                    }
                }
                peer.Reader->ClearAllPartitionStreamEvents();
            }
        }

        Y_UNIT_TEST(WireMalformedKafkaUsesFallbackAndDecodeDoesNotCountAgain) {
            TReaderMetricsTestPeer peer({"topic"}, true);
            peer.AddPartition(1);
            TReaderMetricsTestPeer::TResponse response;
            response.set_bytes_size(101);
            TReaderMetricsTestPeer::AddData(response, 1, 0, "bad header", Ydb::Topic::CODEC_KAFKA_BATCH);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 1);
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 1);
            const auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            UNIT_ASSERT(std::holds_alternative<TReadSessionEvent::TDataReceivedEvent>(*event));
        }

        Y_UNIT_TEST(KafkaReceiveCountSurvivesDecodeFailure) {
            NKafka::TKafkaRecordBatch batch;
            batch.BaseOffset = 0;
            batch.BaseSequence = 0;
            batch.LastOffsetDelta = 2;
            for (int i = 0; i < 3; ++i) {
                NKafka::TKafkaRecord record;
                record.OffsetDelta = i;
                record.SetValue("payload");
                batch.Records.push_back(std::move(record));
            }
            std::string corrupted = NKafka::WriteKafkaRecordBatch(batch);
            const auto header = NKafka::ReadKafkaBatchHeader(corrupted);
            UNIT_ASSERT(header);
            UNIT_ASSERT_VALUES_EQUAL(header->RecordsCount, 3);
            const size_t payloadOffset = static_cast<size_t>(header->Size(
                NKafka::TKafkaRecordBatch::MessageMeta::PresentVersions.Max));
            UNIT_ASSERT(corrupted.size() >= payloadOffset + 10);
            // Leave the valid outer batch header and record-count untouched,
            // but make the first record's varint unparseable.
            for (size_t index = 0; index < 10; ++index) {
                corrupted[payloadOffset + index] = static_cast<char>(0xff);
            }

            TReaderMetricsTestPeer peer({"topic"}, true);
            peer.AddPartition(1);
            TReaderMetricsTestPeer::TResponse response;
            response.set_bytes_size(101);
            TReaderMetricsTestPeer::AddData(
                response, 1, 0, std::move(corrupted), Ydb::Topic::CODEC_KAFKA_BATCH);
            peer.Processor->Reply(std::move(response));
            // The valid batch header admits the three logical records before
            // decoding.  A corrupt payload later leaves one error object.
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("received.messages")->Value(), 3);
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            const auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            const auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(data->GetMessages().size(), 1);
        }

        Y_UNIT_TEST(IncOnlyCounterFallbackPreservesDelta) {
            struct TIncOnlyCounter final: NMetrics::ICounter {
                void Inc() override {
                    ++Value;
                }
                std::uint64_t Value = 0;
            };
            TIncOnlyCounter counter;
            counter.Add(7);
            UNIT_ASSERT_VALUES_EQUAL(counter.Value, 7);
        }

        Y_UNIT_TEST(ReceiveCountersAreVisibleBeforeDecompression) {
            TTopicSdkTestSetup setup("ReceivedCounters.Production");
            setup.Write("payload");

            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);

            const std::string topic = setup.GetTopicPath();
            const std::string consumer = setup.GetConsumerName();
            const std::string reader = "received-reader";

            auto decompressionExecutor = std::make_shared<TManualExecutor>();
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(consumer)
                    .ReaderName(reader)
                    .AppendTopics(TTopicReadSettings(topic))
                    .DecompressionExecutor(decompressionExecutor));

            const auto labels = MakeLabels(setup, topic, consumer, reader);
            const auto receivedMessages = registry->Find("ydb.topic.reader.received.messages", labels);
            const auto delivered = registry->Find("ydb.topic.reader.delivered.messages", labels);
            UNIT_ASSERT(receivedMessages);
            UNIT_ASSERT(delivered);

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto startEvent = session->GetEvent(false);
            UNIT_ASSERT(startEvent);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*startEvent);
            UNIT_ASSERT(start);
            start->Confirm();

            // The response has been accepted and the decompression task has been
            // posted only after the received snapshots have flushed.  No data
            // event can be delivered while the task is held.
            UNIT_ASSERT(decompressionExecutor->WaitForTask());
            UNIT_ASSERT(receivedMessages->WaitForValue(1));
            UNIT_ASSERT_VALUES_EQUAL(receivedMessages->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(delivered->Value(), 0);

            decompressionExecutor->RunOne();
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto dataEvent = session->GetEvent(false);
            UNIT_ASSERT(dataEvent);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*dataEvent);
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(delivered->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(receivedMessages->Value(), 1);

            session->Close(TDuration::Seconds(5));
            session.reset();
            decompressionExecutor->Stop();
            driver.Stop(true);
        }

    } // Y_UNIT_TEST_SUITE(TReceivedCountersMetricsTest)

    Y_UNIT_TEST_SUITE(TReaderNameGateMetricsTest) {
        Y_UNIT_TEST(ReaderMetricsRequireNonemptyNameAcrossAllInstruments) {
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto emptyNameSettings = TReadSessionSettings()
                                         .ConsumerName("consumer")
                                         .AppendTopics(TTopicReadSettings("topic"));
            auto emptyName = TReaderMetrics::Create(
                registry, "endpoint", "/Root", emptyNameSettings);
            UNIT_ASSERT(!emptyName);

            auto registrations = registry->ReaderMetricRegistrations();
            UNIT_ASSERT_VALUES_EQUAL(registrations.Counters, 0);

            const auto explicitSettings = TReadSessionSettings()
                                              .ConsumerName("consumer")
                                              .ReaderName("stable-reader")
                                              .AppendTopics(TTopicReadSettings("topic"));
            const auto explicitMetrics = TReaderMetrics::Create(
                registry, "endpoint", "/Root", explicitSettings);
            UNIT_ASSERT(explicitMetrics);
            const auto explicitTopic = explicitMetrics->ResolveTopic("topic");
            UNIT_ASSERT(explicitTopic);
            UNIT_ASSERT(explicitTopic->DeliveredMessages);
            UNIT_ASSERT(explicitTopic->ReceivedMessages);
            UNIT_ASSERT(explicitTopic->CommitQueued);
            UNIT_ASSERT(explicitTopic->CommitAcknowledged);

            registrations = registry->ReaderMetricRegistrations();
            UNIT_ASSERT(registrations.Counters == 4);
        }

        Y_UNIT_TEST(UnnamedPublicReaderStillDeliversAndCommits) {
            TTopicSdkTestSetup setup("ReaderNameGate.PublicReader");
            setup.Write("payload");

            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            auto session = client.CreateReadSession(TReadSessionSettings()
                                                        .ConsumerName(setup.GetConsumerName())
                                                        .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));

            auto registrations = registry->ReaderMetricRegistrations();
            UNIT_ASSERT_VALUES_EQUAL(registrations.Counters, 0);

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto startEvent = session->GetEvent(false);
            UNIT_ASSERT(startEvent);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*startEvent);
            UNIT_ASSERT(start);
            start->Confirm();

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto dataEvent = session->GetEvent(false);
            UNIT_ASSERT(dataEvent);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*dataEvent);
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(data->GetMessages().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(data->GetMessages().front().GetData(), "payload");
            data->Commit();

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto acknowledgement = session->GetEvent(false);
            UNIT_ASSERT(acknowledgement);
            UNIT_ASSERT(std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&*acknowledgement));

            session->Close(TDuration::Seconds(5));
            session.reset();
            registrations = registry->ReaderMetricRegistrations();
            UNIT_ASSERT_VALUES_EQUAL(registrations.Counters, 0);
            driver.Stop(true);
        }

    } // Y_UNIT_TEST_SUITE(TReaderNameGateMetricsTest)

    Y_UNIT_TEST_SUITE(TCommitQueuedMetricsTest) {
        Y_UNIT_TEST(RegistrationMetadataAndRangeCardinality) {
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto settings = TReadSessionSettings()
                                .WithoutConsumer()
                                .ReaderName("reader")
                                .AppendTopics(TTopicReadSettings("topic"));
            auto metrics = TReaderMetrics::Create(registry, "endpoint", "/Root", settings);
            UNIT_ASSERT(metrics);

            const auto labels = NMetrics::TLabels{
                {"endpoint", "endpoint"},
                {"database", "/Root"},
                {"topic", "topic"},
                {"reader.name", "reader"},
            };
            const auto counter = registry->Find("ydb.topic.reader.commit.queued", labels);
            UNIT_ASSERT(counter);
            UNIT_ASSERT_VALUES_EQUAL(registry->Unit("ydb.topic.reader.commit.queued", labels), "{offset}");
            UNIT_ASSERT(!registry->Description("ydb.topic.reader.commit.queued", labels).empty());
            UNIT_ASSERT(metrics->ResolveTopic("topic") == metrics->ResolveTopic("/Root/topic"));
            UNIT_ASSERT(!registry->Find(
                "ydb.topic.reader.commit.queued",
                {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                 {"consumer", "consumer"},
                 {"reader.name", "reader"}}));

            TTopicOffsets topic;
            topic.Path = "topic";
            TPartitionOffsets partition;
            partition.PartitionId = 0;
            partition.Offsets.push_back({2, 5});
            partition.Offsets.push_back({10, 18});
            topic.Partitions.push_back(std::move(partition));
            std::vector<TTopicOffsets> topics;
            topics.push_back(std::move(topic));
            metrics->RecordCommitQueued(topics);
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 11);

            TTopicOffsets highOffset;
            highOffset.Path = "/Root/topic";
            TPartitionOffsets highPartition;
            highPartition.PartitionId = 3;
            highPartition.Offsets.push_back({std::numeric_limits<ui64>::max() - 5,
                                             std::numeric_limits<ui64>::max() - 2});
            highOffset.Partitions.push_back(std::move(highPartition));
            metrics->RecordCommitQueued({highOffset});
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 14);

            auto saturatedRegistry = std::make_shared<TRecordingMetricRegistry>();
            auto saturatedSettings = TReadSessionSettings()
                                         .WithoutConsumer()
                                         .ReaderName("saturated-reader")
                                         .AppendTopics(TTopicReadSettings("saturated"));
            auto saturatedMetrics = TReaderMetrics::Create(
                saturatedRegistry, "endpoint", "/Root", saturatedSettings);
            UNIT_ASSERT(saturatedMetrics);
            const auto saturatedLabels = NMetrics::TLabels{
                {"endpoint", "endpoint"},
                {"database", "/Root"},
                {"topic", "saturated"},
                {"reader.name", "saturated-reader"},
            };
            const auto saturatedCounter = saturatedRegistry->Find(
                "ydb.topic.reader.commit.queued", saturatedLabels);
            UNIT_ASSERT(saturatedCounter);

            TTopicOffsets saturated;
            saturated.Path = "saturated";
            TPartitionOffsets saturatedPartition;
            saturatedPartition.PartitionId = 4;
            saturatedPartition.Offsets.push_back({0, std::numeric_limits<ui64>::max()});
            saturatedPartition.Offsets.push_back({0, 1});
            saturated.Partitions.push_back(std::move(saturatedPartition));
            saturatedMetrics->RecordCommitQueued({saturated});
            UNIT_ASSERT_VALUES_EQUAL(saturatedCounter->Value(), std::numeric_limits<ui64>::max());
        }

        Y_UNIT_TEST(OrdinaryCommitCountsRequestedRangesAndExcludesWireGaps) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1);
            TReaderMetricsTestPeer::TResponse response;
            auto* partition = response.add_partition_data();
            partition->set_partition_session_id(1);
            auto* batch = partition->add_batches();
            batch->set_codec(Ydb::Topic::CODEC_RAW);
            for (const ui64 offset : {0, 2}) {
                auto* message = batch->add_message_data();
                message->set_offset(offset);
                message->set_data("payload");
                message->set_uncompressed_size(7);
            }
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            data->Commit();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges[0].first, 0);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges[0].second, 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges[1].first, 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges[1].second, 3);
        }

        Y_UNIT_TEST(CompressedMessageCommitCountsRequestedRange) {
            TReaderMetricsTestPeer peer({"topic"}, false);
            peer.AddPartition(1);
            TReaderMetricsTestPeer::TResponse response;
            TReaderMetricsTestPeer::AddData(response, 1, 0, "payload", Ydb::Topic::CODEC_RAW);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            UNIT_ASSERT(data->HasCompressedMessages());
            auto& compressed = data->GetCompressedMessages();
            UNIT_ASSERT_VALUES_EQUAL(compressed.size(), 1);
            compressed.front().Commit();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 1);
        }

        Y_UNIT_TEST(CompressedKafkaCommitCountsLogicalRange) {
            NKafka::TKafkaRecordBatch batch;
            batch.BaseOffset = 7;
            batch.BaseSequence = 0;
            batch.LastOffsetDelta = 2;
            for (int i = 0; i < 3; ++i) {
                NKafka::TKafkaRecord record;
                record.OffsetDelta = i;
                record.SetValue("payload");
                batch.Records.push_back(std::move(record));
            }
            const TString encoded = NKafka::WriteKafkaRecordBatch(batch);

            TReaderMetricsTestPeer peer({"topic"}, false);
            peer.AddPartition(1);
            TReaderMetricsTestPeer::TResponse response;
            TReaderMetricsTestPeer::AddData(
                response, 1, 7, std::string(encoded.data(), encoded.size()), Ydb::Topic::CODEC_KAFKA_BATCH);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            UNIT_ASSERT(data->HasCompressedMessages());
            UNIT_ASSERT_VALUES_EQUAL(data->GetCompressedMessages().front().GetLogicalMessageCount(), 3);
            data->GetCompressedMessages().front().Commit();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 3);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.size(), 1);
            // The protocol-side range is normalized by the test partition
            // stream; commit.queued must use the decoded logical count above.
        }

        Y_UNIT_TEST(DeferredCommitCountsOnlyWhenCommittedAndDoesNotRepeat) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1);
            TReaderMetricsTestPeer::TResponse response;
            TReaderMetricsTestPeer::AddData(response, 1, 4);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            TDeferredCommit deferred;
            deferred.Add(*data);
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 0);
            deferred.Commit();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 1);
            deferred.Commit();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 1);
        }

        Y_UNIT_TEST(StartupConfirmAndStaleClosingAbortingCommitsDoNotCount) {
            {
                TReaderMetricsTestPeer peer;
                peer.StartPartitionFromServer(1, "topic", 42);
                peer.ConfirmStartPartition();
                UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 0);
            }

            for (int mode = 0; mode < 3; ++mode) {
                TReaderMetricsTestPeer peer;
                peer.StartPartitionFromServer(1);
                peer.ConfirmStartPartition();
                TReaderMetricsTestPeer::TResponse response;
                TReaderMetricsTestPeer::AddData(response, 1);
                peer.Processor->Reply(std::move(response));
                UNIT_ASSERT(peer.Executor->WaitForTask());
                peer.Executor->RunOne();
                auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
                UNIT_ASSERT(event);
                auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
                UNIT_ASSERT(data);
                if (mode == 0) {
                    peer.StartPartitionFromServer(1);
                } else if (mode == 1) {
                    peer.Closing();
                } else {
                    peer.Aborting();
                }
                data->Commit();
                UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 0);
            }
        }

        Y_UNIT_TEST(CounterAddCanReenterReaderAfterCommitLocksAreReleased) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1);
            std::atomic<bool> callbackCalled = false;
            auto counter = peer.Counter("commit.queued");
            counter->OnAdd = [&] {
                callbackCalled.store(true, std::memory_order_release);
                peer.AbortReaderForTests();
            };
            TReaderMetricsTestPeer::TResponse response;
            TReaderMetricsTestPeer::AddData(response, 1);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();
            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            auto commitFinished = NThreading::NewPromise<void>();
            std::thread commitThread([data, &commitFinished] {
                data->Commit();
                commitFinished.TrySetValue();
            });
            if (!commitFinished.GetFuture().Wait(TDuration::Seconds(5))) {
                commitThread.detach();
                Y_ABORT("CommitQueued counter reentry did not complete after lock release");
            }
            commitThread.join();
            UNIT_ASSERT(callbackCalled.load(std::memory_order_acquire));
            UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 1);
        }

        Y_UNIT_TEST(DirectReadPublicCommitCountsExactlyOnce) {
            TReaderMetricsTestPeer peer({"topic"}, false, {}, true);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            peer.StartPartitionFromServer(1);
            peer.ConfirmStartPartition();
            TReaderMetricsTestPeer::TDirectResponse response;
            response.set_partition_session_id(1);
            response.set_direct_read_id(1);
            TReaderMetricsTestPeer::TResponse data;
            TReaderMetricsTestPeer::AddData(data, 1, 0);
            *response.mutable_partition_data() = data.partition_data(0);
            peer.ReplyDirect(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* received = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(received);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->DirectReadAcks.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->DirectReadAcks.front().first, 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->DirectReadAcks.front().second, 1);
            received->Commit();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.front().first, 0);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.front().second, 1);
            peer.ReplyCommitAcknowledgement(1, 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 1);
            bool duplicateRejected = false;
            try {
                received->Commit();
            } catch (...) {
                duplicateRejected = true;
            }
            UNIT_ASSERT(duplicateRejected);
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 1);
        }

        Y_UNIT_TEST(RegistrationAndAddFailuresLeaveCommitTransportUnchanged) {
            {
                auto registry = std::make_shared<TRecordingMetricRegistry>(
                    TRecordingMetricRegistry::EFailure::NullCounter, "ydb.topic.reader.commit.queued");
                TReaderMetricsTestPeer peer({"topic"}, false, registry);
                peer.AddPartition(1);
                TReaderMetricsTestPeer::TResponse response;
                TReaderMetricsTestPeer::AddData(response, 1);
                peer.Processor->Reply(std::move(response));
                UNIT_ASSERT(peer.Executor->WaitForTask());
                peer.Executor->RunOne();
                auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
                UNIT_ASSERT(event);
                auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
                UNIT_ASSERT(data);
                data->Commit();
                UNIT_ASSERT(!registry->Find(
                    "ydb.topic.reader.commit.queued",
                    {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                     {"consumer", "consumer"},
                     {"reader.name", "wire-reader"}}));
                UNIT_ASSERT(!peer.Processor->CommitRanges.empty());
            }
            {
                auto registry = std::make_shared<TRecordingMetricRegistry>(
                    TRecordingMetricRegistry::EFailure::Add, "ydb.topic.reader.commit.queued");
                TReaderMetricsTestPeer peer({"topic"}, false, registry);
                peer.AddPartition(1);
                TReaderMetricsTestPeer::TResponse response;
                TReaderMetricsTestPeer::AddData(response, 1);
                peer.Processor->Reply(std::move(response));
                UNIT_ASSERT(peer.Executor->WaitForTask());
                peer.Executor->RunOne();
                auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
                UNIT_ASSERT(event);
                auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
                UNIT_ASSERT(data);
                data->Commit();
                auto counter = registry->Find(
                    "ydb.topic.reader.commit.queued",
                    {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                     {"consumer", "consumer"},
                     {"reader.name", "wire-reader"}});
                UNIT_ASSERT(counter);
                UNIT_ASSERT_VALUES_EQUAL(counter->AddCalls(), 1);
                UNIT_ASSERT_VALUES_EQUAL(counter->Value(), 0);
                UNIT_ASSERT(!peer.Processor->CommitRanges.empty());
            }
            {
                auto registry = std::make_shared<TRecordingMetricRegistry>(
                    TRecordingMetricRegistry::EFailure::NullCounter, "ydb.topic.reader.commit.queued");
                TReaderMetricsTestPeer peer({"topic"}, false, registry);
                peer.AddPartition(1);
                TReaderMetricsTestPeer::TResponse response;
                TReaderMetricsTestPeer::AddData(response, 1);
                peer.Processor->Reply(std::move(response));
                UNIT_ASSERT(peer.Executor->WaitForTask());
                peer.Executor->RunOne();

                TTopicOffsets topic;
                topic.Path = "topic";
                TPartitionOffsets partition;
                partition.PartitionId = 1;
                partition.Offsets.push_back({0, 1});
                topic.Partitions.push_back(std::move(partition));
                {
                    peer.Metrics->RecordCommitQueued({topic});
                }
                // No CommitQueued registration means the transaction helper
                // does not attempt to export the transaction offsets.
                UNIT_ASSERT(!registry->Find(
                    "ydb.topic.reader.commit.queued",
                    {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                     {"consumer", "consumer"},
                     {"reader.name", "wire-reader"}}));
            }
        }

    } // Y_UNIT_TEST_SUITE(TCommitQueuedMetricsTest)

    Y_UNIT_TEST_SUITE(TCommitAcknowledgedMetricsTest) {
        Y_UNIT_TEST(RegistersOffsetCounterOnlyForExplicitReaderName) {
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto settings = TReadSessionSettings()
                                .WithoutConsumer()
                                .ReaderName("stable-reader")
                                .AppendTopics(TTopicReadSettings("topic"));
            auto metrics = TReaderMetrics::Create(registry, "endpoint", "/Root", settings);
            UNIT_ASSERT(metrics);

            const NMetrics::TLabels labels = {
                {"endpoint", "endpoint"},
                {"database", "/Root"},
                {"topic", "topic"},
                {"reader.name", "stable-reader"},
            };
            auto acknowledged = registry->Find(
                "ydb.topic.reader.commit.acknowledged", labels);
            UNIT_ASSERT(acknowledged);
            UNIT_ASSERT_VALUES_EQUAL(
                registry->Unit("ydb.topic.reader.commit.acknowledged", labels), "{offset}");
            UNIT_ASSERT_VALUES_EQUAL(
                registry->Description("ydb.topic.reader.commit.acknowledged", labels),
                "Number of ordinary application-requested offset positions acknowledged by the Topic Reader; "
                "transactional offsets are excluded.");
        }

        Y_UNIT_TEST(CountsDistinctUserRangesCoveredByControlAcknowledgements) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            auto acknowledged = peer.Registry->Find(
                "ydb.topic.reader.commit.acknowledged",
                {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                 {"consumer", "consumer"},
                 {"reader.name", "wire-reader"}});
            UNIT_ASSERT(acknowledged);

            peer.ReplyCommitAcknowledgement(1, 100);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
            peer.CommitRangeForTests(1, 100, 102);
            peer.CommitRangeForTests(1, 105, 108);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);

            peer.ReplyCommitAcknowledgement(1, 106);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 3);
            peer.ReplyCommitAcknowledgement(1, 106);
            peer.ReplyCommitAcknowledgement(1, 104);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 3);
            peer.ReplyCommitAcknowledgement(1, 108);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 5);
            peer.ReplyCommitAcknowledgement(1, 1000);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 5);
        }

        Y_UNIT_TEST(NegativeSignedAcknowledgementDoesNotCount) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            peer.CommitRangeForTests(1, 100, 102);

            peer.ReplyCommitAcknowledgement(1, -1);

            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
        }

        Y_UNIT_TEST(LargeValidSignedAcknowledgementDoesNotOverflow) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 0);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            peer.CommitRangeForTests(1, 0, 1);

            peer.ReplyCommitAcknowledgement(1, std::numeric_limits<i64>::max());

            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 1);
        }

        Y_UNIT_TEST(NonSuccessControlEnvelopeDoesNotCountAcknowledgement) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            peer.CommitRangeForTests(1, 100, 102);

            peer.ReplyCommitAcknowledgement(1, 102, Ydb::StatusIds::BAD_REQUEST);

            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
        }

        Y_UNIT_TEST(ReconnectBaselineDoesNotReconstructLostAcknowledgement) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            peer.CommitRangeForTests(1, 100, 102);

            // A new incarnation reports the already advanced server baseline,
            // but that is not proof this Reader received the old ACK.
            peer.StopPartitionFromServer(1, false);
            auto closed = peer.Queue->GetEvent(
                false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(closed);
            UNIT_ASSERT(std::get_if<TReadSessionEvent::TPartitionSessionClosedEvent>(&*closed));
            peer.StartPartitionFromServer(1, "topic", 102);
            peer.ConfirmStartPartition();
            peer.ReplyCommitAcknowledgement(1, 102);

            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
        }

        Y_UNIT_TEST(MutateThenThrowAcknowledgementExportIsNotReplayed) {
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            TReaderMetricsTestPeer peer({"topic"}, false, registry);
            peer.AddPartition(1, "topic", 100);
            auto acknowledged = peer.Counter("commit.acknowledged");
            acknowledged->Failure = ECounterFailure::AfterAdd;
            peer.CommitRangeForTests(1, 100, 102);

            peer.ReplyCommitAcknowledgement(1, 102);
            peer.ReplyCommitAcknowledgement(1, 102);

            UNIT_ASSERT_VALUES_EQUAL(acknowledged->AddCalls(), 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 2);
        }

        Y_UNIT_TEST(AcknowledgementsRouteToTheirCanonicalTopicSeries) {
            TReaderMetricsTestPeer peer({"topic", "other"});
            peer.AddPartition(1, "topic", 100);
            peer.AddPartition(2, "other", 200);
            const auto first = peer.Counter("commit.acknowledged", "topic");
            const auto second = peer.Counter("commit.acknowledged", "other");
            peer.CommitRangeForTests(1, 100, 102);
            peer.CommitRangeForTests(2, 200, 203);

            peer.ReplyCommitAcknowledgement(1, 102);
            UNIT_ASSERT_VALUES_EQUAL(first->Value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(second->Value(), 0);
            peer.ReplyCommitAcknowledgement(2, 203);
            UNIT_ASSERT_VALUES_EQUAL(first->Value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(second->Value(), 3);
        }

        Y_UNIT_TEST(UnknownPartitionAndEmptyAcknowledgementDoNotCount) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            peer.CommitRangeForTests(1, 100, 102);

            peer.ReplyCommitAcknowledgement(999, 102);
            TServerMessage<false> emptyAck;
            emptyAck.set_status(Ydb::StatusIds::SUCCESS);
            emptyAck.mutable_commit_offset_response();
            peer.Processor->ReplyMessage(std::move(emptyAck));
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
            peer.ReplyCommitAcknowledgement(1, 102);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 2);
        }

        Y_UNIT_TEST(GracefulStopRemainsEligibleUntilConfirmed) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            peer.CommitRangeForTests(1, 100, 102);
            peer.StopPartitionFromServer(1, true);

            auto event = peer.Queue->GetEvent(false);
            UNIT_ASSERT(event);
            auto* stop = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&*event);
            UNIT_ASSERT(stop);
            peer.ReplyCommitAcknowledgement(1, 102);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 2);
            stop->Confirm();
        }

        Y_UNIT_TEST(ForcefulStopPreventsLateAcknowledgement) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            peer.CommitRangeForTests(1, 100, 102);
            peer.StopPartitionFromServer(1, false);

            peer.ReplyCommitAcknowledgement(1, 102);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
        }

        Y_UNIT_TEST(RegistrationFailureForAcknowledgedCounterLeavesOtherMetricsAvailable) {
            constexpr char acknowledgedName[] = "ydb.topic.reader.commit.acknowledged";
            const NMetrics::TLabels labels = {
                {"endpoint", "endpoint"},
                {"database", "/Root"},
                {"topic", "topic"},
                {"consumer", "consumer"},
                {"reader.name", "wire-reader"},
            };

            for (const bool returnNull : {false, true}) {
                auto registry = std::make_shared<TRecordingMetricRegistry>(
                    returnNull ? TRecordingMetricRegistry::EFailure::NullCounter
                               : TRecordingMetricRegistry::EFailure::Registration,
                    std::string(acknowledgedName));
                TReaderMetricsTestPeer peer({"topic"}, false, registry);
                peer.AddPartition(1, "topic", 100);

                UNIT_ASSERT(!registry->Find(std::string(acknowledgedName), labels));
                UNIT_ASSERT(registry->Find("ydb.topic.reader.commit.queued", labels));

                peer.CommitRangeForTests(1, 100, 102);
                peer.ReplyCommitAcknowledgement(1, 102);
                UNIT_ASSERT(registry->Find("ydb.topic.reader.commit.queued", labels));
            }
        }

        Y_UNIT_TEST(AddFailureDoesNotEscapeOrReplayAcknowledgedRange) {
            auto registry = std::make_shared<TRecordingMetricRegistry>(
                TRecordingMetricRegistry::EFailure::Add, "ydb.topic.reader.commit.acknowledged");
            TReaderMetricsTestPeer peer({"topic"}, false, registry);
            peer.AddPartition(1, "topic", 100);
            auto acknowledged = registry->Find(
                "ydb.topic.reader.commit.acknowledged",
                {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                 {"consumer", "consumer"},
                 {"reader.name", "wire-reader"}});
            UNIT_ASSERT(acknowledged);

            peer.CommitRangeForTests(1, 100, 102);
            peer.ReplyCommitAcknowledgement(1, 102);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->AddCalls(), 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);

            // The failed export is drop-only: an equal or later ACK cannot replay
            // the already-consumed user range.
            peer.ReplyCommitAcknowledgement(1, 102);
            peer.ReplyCommitAcknowledgement(1, 103);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->AddCalls(), 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
        }

        Y_UNIT_TEST(AdjacentApplicationRangesAreCoalescedForAcknowledgement) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            auto acknowledged = peer.Registry->Find(
                "ydb.topic.reader.commit.acknowledged",
                {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                 {"consumer", "consumer"},
                 {"reader.name", "wire-reader"}});
            UNIT_ASSERT(acknowledged);

            peer.CommitRangeForTests(1, 100, 102);
            peer.CommitRangeForTests(1, 102, 104);
            peer.ReplyCommitAcknowledgement(1, 104);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 4);
        }

        Y_UNIT_TEST(StaleControlResponseDoesNotCountAcknowledgement) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 100);
            auto acknowledged = peer.Registry->Find(
                "ydb.topic.reader.commit.acknowledged",
                {{"endpoint", "endpoint"}, {"database", "/Root"}, {"topic", "topic"},
                 {"consumer", "consumer"},
                 {"reader.name", "wire-reader"}});
            UNIT_ASSERT(acknowledged);

            peer.CommitRangeForTests(1, 100, 102);
            peer.Stale();
            peer.ReplyCommitAcknowledgement(1, 102);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);
        }

        Y_UNIT_TEST(PublicDataEventCommitExportsOnlyRequestedPositionsAcrossWireGap) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            const auto queued = peer.Counter("commit.queued");

            TReaderMetricsTestPeer::TResponse response;
            auto* partition = response.add_partition_data();
            partition->set_partition_session_id(1);
            auto* batch = partition->add_batches();
            batch->set_codec(Ydb::Topic::CODEC_RAW);
            for (const ui64 offset : {0, 2}) {
                auto* message = batch->add_message_data();
                message->set_offset(offset);
                message->set_data("payload");
                message->set_uncompressed_size(7);
            }
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            data->Commit();

            UNIT_ASSERT_VALUES_EQUAL(queued->Value(), 2);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges[0].first, 0);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges[0].second, 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges[1].first, 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges[1].second, 3);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);

            peer.ReplyCommitAcknowledgement(1, 3);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 2);
            auto acknowledgementEvent = peer.Queue->GetEvent(
                false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(acknowledgementEvent);
            const auto* acknowledgement =
                std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&*acknowledgementEvent);
            UNIT_ASSERT(acknowledgement);
            UNIT_ASSERT_VALUES_EQUAL(acknowledgement->GetCommittedOffset(), 3);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 2);
        }

        Y_UNIT_TEST(PublicMessageCommitAndControlAcknowledgementUseWirePath) {
            TReaderMetricsTestPeer peer({"topic"}, true);
            peer.AddPartition(1, "topic", 50);
            const auto acknowledged = peer.Counter("commit.acknowledged");

            TReaderMetricsTestPeer::TResponse response;
            TReaderMetricsTestPeer::AddData(response, 1, 50);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(data->GetMessages().size(), 1);
            data->GetMessages().front().Commit();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.front().first, 50);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.front().second, 51);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);

            peer.ReplyCommitAcknowledgement(1, 51);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 1);
        }

        Y_UNIT_TEST(CompressedMessageCommitCountsLogicalOffsetsOnControlAcknowledgement) {
            NKafka::TKafkaRecordBatch batch;
            batch.BaseOffset = 7;
            batch.BaseSequence = 0;
            batch.LastOffsetDelta = 2;
            for (int i = 0; i < 3; ++i) {
                NKafka::TKafkaRecord record;
                record.OffsetDelta = i;
                record.SetValue("payload");
                batch.Records.push_back(std::move(record));
            }
            const TString encoded = NKafka::WriteKafkaRecordBatch(batch);

            TReaderMetricsTestPeer peer({"topic"}, false);
            peer.AddPartition(1, "topic", 7);
            const auto acknowledged = peer.Counter("commit.acknowledged");
            TReaderMetricsTestPeer::TResponse response;
            TReaderMetricsTestPeer::AddData(
                response, 1, 7, std::string(encoded.data(), encoded.size()),
                Ydb::Topic::CODEC_KAFKA_BATCH);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            UNIT_ASSERT(data->HasCompressedMessages());
            auto& compressedMessages = data->GetCompressedMessages();
            UNIT_ASSERT_VALUES_EQUAL(compressedMessages.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(compressedMessages.front().GetLogicalMessageCount(), 3);
            compressedMessages.front().Commit();

            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 3);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.front().first, 7);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.front().second, 10);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);

            peer.ReplyCommitAcknowledgement(1, 10);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 3);
        }

        Y_UNIT_TEST(DeferredCommitExportsOnlyWhenItsWireCommitIsAcknowledged) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1, "topic", 4);
            const auto acknowledged = peer.Counter("commit.acknowledged");

            TReaderMetricsTestPeer::TResponse response;
            TReaderMetricsTestPeer::AddData(response, 1, 4);
            peer.Processor->Reply(std::move(response));
            UNIT_ASSERT(peer.Executor->WaitForTask());
            peer.Executor->RunOne();

            auto event = peer.Queue->GetEvent(false, std::numeric_limits<size_t>::max());
            UNIT_ASSERT(event);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event);
            UNIT_ASSERT(data);
            TDeferredCommit deferred;
            deferred.Add(*data);
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 0);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);

            deferred.Commit();
            UNIT_ASSERT_VALUES_EQUAL(peer.Counter("commit.queued")->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.front().first, 4);
            UNIT_ASSERT_VALUES_EQUAL(peer.Processor->CommitRanges.front().second, 5);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);

            peer.ReplyCommitAcknowledgement(1, 5);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 1);
        }

        Y_UNIT_TEST(PublicReaderCountsARealServiceControlAcknowledgement) {
            TTopicSdkTestSetup setup("CommitAcknowledged.RealService");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            const std::string reader = "service-ack-reader";
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(setup.GetConsumerName())
                    .ReaderName(reader)
                    .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
            const auto labels = MakeLabels(
                setup, setup.GetTopicPath(), setup.GetConsumerName(), reader);
            const auto acknowledged = registry->Find(
                "ydb.topic.reader.commit.acknowledged", labels);
            const auto queued = registry->Find("ydb.topic.reader.commit.queued", labels);
            UNIT_ASSERT(acknowledged);
            UNIT_ASSERT(queued);

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto startEvent = session->GetEvent(false);
            UNIT_ASSERT(startEvent);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*startEvent);
            UNIT_ASSERT(start);
            start->Confirm();

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto dataEvent = session->GetEvent(false);
            UNIT_ASSERT(dataEvent);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*dataEvent);
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(data->GetMessagesCount(), 1);
            data->Commit();

            UNIT_ASSERT(queued->WaitForValue(1));
            UNIT_ASSERT(acknowledged->WaitForValue(1));
            UNIT_ASSERT_VALUES_EQUAL(queued->Value(), 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 1);

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto acknowledgementEvent = session->GetEvent(false);
            UNIT_ASSERT(acknowledgementEvent);
            auto* acknowledgement =
                std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&*acknowledgementEvent);
            UNIT_ASSERT(acknowledgement);
            UNIT_ASSERT(acknowledgement->GetCommittedOffset() >= 1);
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 1);

            session->Close(TDuration::Seconds(5));
            session.reset();
            driver.Stop(true);
        }

        Y_UNIT_TEST(ConsumerlessServiceReaderDeliversWithoutCommitAcknowledgement) {
            TTopicSdkTestSetup setup("CommitAcknowledged.ConsumerlessService");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            const std::string reader = "service-consumerless-ack-reader";
            auto topicSettings = TTopicReadSettings(setup.GetTopicPath());
            topicSettings.AppendPartitionIds(0);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .WithoutConsumer()
                    .ReaderName(reader)
                    .AppendTopics(topicSettings));
            const NMetrics::TLabels labels = {
                {"endpoint", setup.GetEndpoint()},
                {"database", setup.GetDatabase()},
                {"topic", setup.GetTopicPath()},
                {"reader.name", reader},
            };
            const auto acknowledged = registry->Find(
                "ydb.topic.reader.commit.acknowledged", labels);
            UNIT_ASSERT(acknowledged);

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto startEvent = session->GetEvent(false);
            UNIT_ASSERT(startEvent);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*startEvent);
            UNIT_ASSERT(start);
            start->Confirm();

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto dataEvent = session->GetEvent(false);
            UNIT_ASSERT(dataEvent);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*dataEvent);
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(data->GetMessagesCount(), 1);
            data->Commit();

            std::optional<TSessionClosedEvent> rejected;
            const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
            while (!rejected.has_value() && TInstant::Now() < deadline) {
                if (!session->WaitEvent().Wait(TDuration::Seconds(1))) {
                    continue;
                }
                auto rejectionEvent = session->GetEvent(false);
                UNIT_ASSERT(rejectionEvent);
                if (auto* sessionClosed = std::get_if<TSessionClosedEvent>(&*rejectionEvent)) {
                    rejected.emplace(*sessionClosed);
                }
            }
            UNIT_ASSERT_C(rejected.has_value(), "timed out waiting for consumerless commit rejection");
            UNIT_ASSERT_VALUES_EQUAL(rejected->GetStatus(), EStatus::BAD_REQUEST);
            UNIT_ASSERT_STRING_CONTAINS(
                rejected->GetIssues().ToOneLineString(), "can't commit when reading without a consumer");
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 0);

            session->Close(TDuration::Seconds(5));
            session.reset();
            driver.Stop(true);
        }

        Y_UNIT_TEST(AcknowledgedCallbackKeepsQueueAliveUntilDeferredWaiterIsSignalled) {
            TReaderMetricsTestPeer peer;
            peer.AddPartition(1);
            peer.CommitRangeForTests(1, 0, 1);
            const std::weak_ptr<TReadSessionEventsQueue<false>> queue = peer.Queue;
            const std::weak_ptr<TSingleClusterReadSessionImpl<false>> reader = peer.Reader;
            auto ready = peer.Queue->WaitEvent();
            UNIT_ASSERT(!ready.HasValue());
            bool signalledWhileAlive = false;
            ready.Subscribe([&](const auto&) {
                signalledWhileAlive = !queue.expired() && !reader.expired();
            });
            auto acknowledged = peer.Counter("commit.acknowledged");
            acknowledged->OnAdd = [&] {
                // Cancel drops the context's ownership; no test fixture owner
                // may keep the session or queue alive for the deferred waiter.
                peer.DestroyReaderForTests();
                // Stop the negative control before it dereferences a freed queue.
                // Counter callbacks swallow exceptions, so use a fatal assertion.
                Y_ABORT_UNLESS(!queue.expired(), "ACK deferred waiter outlived its event queue");
                Y_ABORT_UNLESS(!ready.HasValue(), "ACK waiter was signalled before metric export completed");
            };

            peer.ReplyCommitAcknowledgement(1, 1);

            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 1);
            UNIT_ASSERT(ready.HasValue());
            UNIT_ASSERT(signalledWhileAlive);
            UNIT_ASSERT(reader.expired());
            UNIT_ASSERT(queue.expired());
        }

        Y_UNIT_TEST(RealReaderCanBeDestroyedFromAcknowledgedCounter) {
            TTopicSdkTestSetup setup("CommitAcknowledged.ReentrantDestroy");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            const std::string reader = "destroy-in-ack-reader";
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(setup.GetConsumerName())
                    .ReaderName(reader)
                    .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
            auto acknowledged = registry->Find(
                "ydb.topic.reader.commit.acknowledged",
                MakeLabels(setup, setup.GetTopicPath(), setup.GetConsumerName(), reader));
            UNIT_ASSERT(acknowledged);

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto startEvent = session->GetEvent(false);
            UNIT_ASSERT(startEvent);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*startEvent);
            UNIT_ASSERT(start);
            start->Confirm();
            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto dataEvent = session->GetEvent(false);
            UNIT_ASSERT(dataEvent);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*dataEvent);
            UNIT_ASSERT(data);
            UNIT_ASSERT_VALUES_EQUAL(data->GetMessagesCount(), 1);

            auto ready = session->WaitEvent();
            UNIT_ASSERT(!ready.HasValue());
            const std::weak_ptr<IReadSession> lifetime = session;
            auto destroyed = NThreading::NewPromise<void>();
            acknowledged->OnAdd = [owner = std::move(session), destroyed]() mutable {
                owner.reset();
                destroyed.TrySetValue();
            };
            data->Commit();

            Y_ABORT_UNLESS(destroyed.GetFuture().Wait(TDuration::Seconds(10)),
                "Destroying the last Reader owner from the acknowledged counter deadlocked");
            Y_ABORT_UNLESS(ready.Wait(TDuration::Seconds(5)), "Deferred ACK waiter was not signalled");
            UNIT_ASSERT(lifetime.expired());
            UNIT_ASSERT_VALUES_EQUAL(acknowledged->Value(), 1);
            driver.Stop(true);
        }

        Y_UNIT_TEST(RealReaderCanCloseReentrantlyFromAcknowledgedCounter) {
            TTopicSdkTestSetup setup("CommitAcknowledged.ReentrantClose");
            setup.Write("payload");
            auto registry = std::make_shared<TRecordingMetricRegistry>();
            auto config = setup.MakeDriverConfig();
            config.SetMetricRegistry(registry);
            TDriver driver(std::move(config));
            TTopicClient client(driver);
            const std::string reader = "reentrant-ack-reader";
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(setup.GetConsumerName())
                    .ReaderName(reader)
                    .AppendTopics(TTopicReadSettings(setup.GetTopicPath())));
            auto acknowledged = registry->Find(
                "ydb.topic.reader.commit.acknowledged",
                MakeLabels(setup, setup.GetTopicPath(), setup.GetConsumerName(), reader));
            UNIT_ASSERT(acknowledged);
            auto closeCompleted = NThreading::NewPromise<void>();
            acknowledged->OnAdd = [weak = std::weak_ptr<IReadSession>(session), closeCompleted]() mutable {
                if (auto readerSession = weak.lock()) {
                    readerSession->Close(TDuration::MilliSeconds(1));
                }
                closeCompleted.TrySetValue();
            };

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto startEvent = session->GetEvent(false);
            UNIT_ASSERT(startEvent);
            auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*startEvent);
            UNIT_ASSERT(start);
            start->Confirm();

            UNIT_ASSERT(session->WaitEvent().Wait(TDuration::Seconds(5)));
            auto dataEvent = session->GetEvent(false);
            UNIT_ASSERT(dataEvent);
            auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*dataEvent);
            UNIT_ASSERT(data);
            data->Commit();

            Y_ABORT_UNLESS(acknowledged->WaitForValue(1),
                           "Real Topic service did not produce a commit ACK");
            Y_ABORT_UNLESS(closeCompleted.GetFuture().Wait(TDuration::Seconds(10)),
                           "Acknowledged metric reentrant public Close deadlocked");
            session.reset();
            driver.Stop(true);
        }

    } // Y_UNIT_TEST_SUITE(TCommitAcknowledgedMetricsTest)

} // namespace NYdb::inline Dev::NTopic::NTests
