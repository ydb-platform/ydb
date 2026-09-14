#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/read_session_impl.ipp>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections/grpc_connections.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <string_view>

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace std::chrono_literals;

namespace {

    class TPromiseLogBackend final: public TLogBackend {
    public:
        explicit TPromiseLogBackend(std::shared_ptr<std::promise<void>> observed)
            : Observed_(std::move(observed))
        {
        }

        void WriteData(const TLogRecord& record) override {
            if (std::string_view(record.Data, record.Len).starts_with("runtime-lifetime Counters: {") && !Signalled_.exchange(true))
            {
                Observed_->set_value();
            }
        }

        void ReopenLog() override {
        }

    private:
        const std::shared_ptr<std::promise<void>> Observed_;
        std::atomic_bool Signalled_ = false;
    };

    class TPartitionLookupService final: public Ydb::Topic::V1::TopicService::Service {
    public:
        grpc::Status DescribePartition(grpc::ServerContext*, const Ydb::Topic::DescribePartitionRequest* request,
                                       Ydb::Topic::DescribePartitionResponse* response) override {
            Entered.TrySetValue();
            if (!Release.GetFuture().Wait(TDuration::Seconds(10))) {
                return {grpc::StatusCode::DEADLINE_EXCEEDED, "Partition lookup was not released"};
            }
            Ydb::Topic::DescribePartitionResult result;
            auto* partition = result.mutable_partition();
            partition->set_partition_id(request->partition_id());
            partition->set_active(true);
            partition->mutable_partition_location()->set_node_id(17);
            partition->mutable_partition_location()->set_generation(1);
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            operation->mutable_result()->PackFrom(result);
            return grpc::Status::OK;
        }

        NThreading::TPromise<void> Entered = NThreading::NewPromise();
        NThreading::TPromise<void> Release = NThreading::NewPromise();
    };

    class TEndpointLookupService final: public Ydb::Discovery::V1::DiscoveryService::Service {
    public:
        grpc::Status ListEndpoints(grpc::ServerContext*, const Ydb::Discovery::ListEndpointsRequest*,
                                   Ydb::Discovery::ListEndpointsResponse* response) override {
            Y_SCOPE_EXIT(done = Finished) {
                done.TrySetValue();
            };
            Entered.TrySetValue();
            if (!Release.GetFuture().Wait(TDuration::Seconds(10))) {
                return {grpc::StatusCode::DEADLINE_EXCEEDED, "Endpoint lookup was not released"};
            }
            Ydb::Discovery::ListEndpointsResult result;
            auto* endpoint = result.add_endpoints();
            endpoint->set_address("127.0.0.1");
            endpoint->set_port(Port);
            endpoint->set_node_id(17);
            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            operation->mutable_result()->PackFrom(result);
            return grpc::Status::OK;
        }

        int Port = 0;
        NThreading::TPromise<void> Entered = NThreading::NewPromise();
        NThreading::TPromise<void> Release = NThreading::NewPromise();
        NThreading::TPromise<void> Finished = NThreading::NewPromise();
    };

    class TWriterLifetimeServer {
    public:
        TWriterLifetimeServer() {
            grpc::ServerBuilder builder;
            builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &Discovery.Port);
            builder.RegisterService(&Topic);
            builder.RegisterService(&Discovery);
            Server_ = builder.BuildAndStart();
            UNIT_ASSERT(Server_);
            Endpoint = "127.0.0.1:" + std::to_string(Discovery.Port);
        }

        ~TWriterLifetimeServer() {
            Release();
            Server_->Shutdown(std::chrono::system_clock::now());
            Server_->Wait();
        }

        void Release() {
            Topic.Release.TrySetValue();
            Discovery.Release.TrySetValue();
        }

        TPartitionLookupService Topic;
        TEndpointLookupService Discovery;
        std::string Endpoint;

    private:
        std::unique_ptr<grpc::Server> Server_;
    };

} // namespace

Y_UNIT_TEST_SUITE(TopicRuntimeLifetime) {
    Y_UNIT_TEST(DeferredCallbackOwnsCaptureAndRunsOnceAfterUnlock) {
        auto payload = std::make_shared<int>(42);
        std::weak_ptr<int> weak = payload;
        std::mutex mutex;
        bool locked = false;
        bool ranUnlocked = false;
        bool sawPayload = false;
        unsigned calls = 0;
        {
            TDeferredActions<false> deferred;
            {
                std::lock_guard lock(mutex);
                locked = true;
                deferred.DeferCallback([capture = payload, &mutex, &locked, &ranUnlocked, &sawPayload, &calls] {
                    ++calls;
                    sawPayload = *capture == 42;
                    // An incorrectly immediate callback must not try to relock its own mutex.
                    if (!locked) {
                        std::unique_lock lock(mutex, std::try_to_lock);
                        ranUnlocked = lock.owns_lock();
                    }
                });
                payload.reset();
            }
            locked = false;
            UNIT_ASSERT(!weak.expired());
            UNIT_ASSERT_VALUES_EQUAL(calls, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
        UNIT_ASSERT(sawPayload);
        UNIT_ASSERT(ranUnlocked);
        UNIT_ASSERT(weak.expired());
    }

    Y_UNIT_TEST(CountersLoggerSchedulesAfterDriverStopAndDestruction) {
        auto observed = std::make_shared<std::promise<void>>();
        auto future = observed->get_future();
        bool logged = false;
        {
            auto driver = std::make_unique<TDriver>(TDriverConfig()
                                                        .SetEndpoint("localhost:1")
                                                        .SetDiscoveryMode(EDiscoveryMode::Off));
            TLog log(MakeHolder<TPromiseLogBackend>(observed));
            auto counters = MakeIntrusive<TReaderCounters>(MakeIntrusive<::NMonitoring::TDynamicCounters>());
            auto logger = std::make_shared<TCountersLogger<false>>(
                CreateInternalInterface(*driver), std::vector<TCallbackContextPtr<false>>{},
                counters, log, "runtime-lifetime ", TInstant::Now());
            auto context = logger->MakeCallbackContext();
            Y_DEFER {
                // Wait for an active callback before the synchronous final dump.
                context->Cancel();
                logger->Stop();
            };

            driver->Stop(false);
            driver->Stop(true);
            driver.reset();
            logger->Start();
            // Observe the real scheduled dump before Stop can emit its final dump.
            logged = future.wait_for(10s) == std::future_status::ready;
        }
        UNIT_ASSERT_C(logged, "The counters logger did not run after driver destruction");
    }
    Y_UNIT_TEST(WriterCanCloseDuringDiscoveryAfterDriverDestruction) {
        TWriterLifetimeServer server;
        auto driver = std::make_unique<TDriver>(TDriverConfig()
                                                    .SetEndpoint(server.Endpoint)
                                                    .SetDatabase("/Root")
                                                    .SetDiscoveryMode(EDiscoveryMode::Off));
        auto client = std::make_unique<TTopicClient>(*driver);
        auto closed = NThreading::NewPromise<EStatus>();
        auto settings = TWriteSessionSettings()
                            .Path("topic")
                            .ProducerId("lifetime-test")
                            .PartitionId(0)
                            .DirectWriteToPartition(true)
                            .Codec(ECodec::RAW);
        settings.EventHandlers_.SessionClosedHandler([closed](const TSessionClosedEvent& event) mutable {
            closed.TrySetValue(event.GetStatus());
        });
        auto writer = client->CreateWriteSession(settings);
        Y_SCOPE_EXIT(&server) {
            server.Release();
        };
        auto initialized = writer->GetInitSeqNo();
        UNIT_ASSERT(server.Topic.Entered.GetFuture().Wait(TDuration::Seconds(10)));
        client.reset();
        driver->Stop(true);
        driver.reset();

        // The returned partition node is not in the endpoint pool: refresh must
        // still start, while explicit session close must not wait for discovery.
        server.Topic.Release.SetValue();
        UNIT_ASSERT(server.Discovery.Entered.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT(!initialized.IsReady());
        UNIT_ASSERT(writer->Close(TDuration::Zero()));
        UNIT_ASSERT(initialized.Wait(TDuration::Seconds(10)));
        UNIT_ASSERT_EXCEPTION_CONTAINS(initialized.GetValue(), yexception, "session closed");
        UNIT_ASSERT(closed.GetFuture().Wait(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(closed.GetFuture().GetValue(), EStatus::SUCCESS);
        writer.reset();
        UNIT_ASSERT(!server.Discovery.Finished.GetFuture().IsReady());
    }
} // Y_UNIT_TEST_SUITE(TopicRuntimeLifetime)
