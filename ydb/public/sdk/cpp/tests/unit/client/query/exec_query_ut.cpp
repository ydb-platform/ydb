#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/completion_queue.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/async_stream.h>

#include <util/string/builder.h>

#include <chrono>
#include <memory>
#include <thread>
#include <utility>

using namespace NYdb;
using namespace NYdb::NQuery;

namespace {

    constexpr TDuration WaitTimeout = TDuration::Seconds(10);

    // A single-call server driven by completion-queue events. It sends no response
    // parts until instructed, so a ReadNext() is guaranteed to stay pending.
    class TQueryStreamServer {
    public:
        TQueryStreamServer()
            : Writer_(&Context_)
        {
            NTesting::InitPortManagerFromEnv();
            const auto port = NTesting::GetFreePort();
            const auto endpoint = TStringBuilder() << "127.0.0.1:" << port;

            grpc::ServerBuilder builder;
            builder.AddListeningPort(endpoint, grpc::InsecureServerCredentials());
            builder.RegisterService(&Service_);
            Queue_ = builder.AddCompletionQueue();
            Server_ = builder.BuildAndStart();
            UNIT_ASSERT(Server_);

            Context_.AsyncNotifyWhenDone(&Done_);
            Service_.RequestExecuteQuery(&Context_, &Request_, &Writer_, Queue_.get(), Queue_.get(), &Accepted_);

            Driver_ = std::make_unique<TDriver>(TDriverConfig()
                                                    .SetEndpoint(endpoint)
                                                    .SetDiscoveryMode(EDiscoveryMode::Off)
                                                    .SetDatabase("/Root"));
            Client_ = std::make_unique<TQueryClient>(*Driver_);
        }

        ~TQueryStreamServer() {
            // Also unblock cleanup if a test assertion fails with a live RPC.
            Server_->Shutdown(std::chrono::system_clock::now());
            Queue_->Shutdown();
            void* tag = nullptr;
            bool ok = false;
            while (Queue_->Next(&tag, &ok)) {
            }
            Client_.reset();
            Driver_->Stop(true);
        }

        TExecuteQueryIterator Start(const TExecuteQuerySettings& settings = {}) {
            auto stream = Client_->StreamExecuteQuery("SELECT 1", TTxControl::NoTx(), settings);
            WaitFor(Accepted_);
            UNIT_ASSERT(Accepted_.Ok);
            Writer_.SendInitialMetadata(&Metadata_);
            WaitFor(Metadata_);
            UNIT_ASSERT(Metadata_.Ok);
            UNIT_ASSERT(stream.Wait(WaitTimeout));
            auto iterator = stream.ExtractValueSync();
            UNIT_ASSERT(iterator.IsSuccess());
            return iterator;
        }

        void WaitForCancellation() {
            WaitFor(Done_);
            UNIT_ASSERT(Context_.IsCancelled());
            // The client must observe cancellation before the server finishes the RPC.
            // This keeps cancellation independent of a cooperative server response.
        }

        void WriteSuccess() {
            Ydb::Query::ExecuteQueryResponsePart response;
            response.set_status(Ydb::StatusIds::SUCCESS);
            Writer_.Write(response, &Written_);
            WaitFor(Written_);
            UNIT_ASSERT(Written_.Ok);
        }

        void Finish() {
            Writer_.Finish(grpc::Status::OK, &Finished_);
            WaitFor(Finished_);
            WaitFor(Done_);
        }

    private:
        struct TTag {
            bool Complete = false;
            bool Ok = false;
        };

        void WaitFor(TTag& expected) {
            const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
            while (!expected.Complete) {
                void* tag = nullptr;
                bool ok = false;
                UNIT_ASSERT(Queue_->AsyncNext(&tag, &ok, deadline) == grpc::CompletionQueue::GOT_EVENT);
                auto& received = *static_cast<TTag*>(tag);
                UNIT_ASSERT(!received.Complete);
                received.Complete = true;
                received.Ok = ok;
            }
        }

        Ydb::Query::V1::QueryService::AsyncService Service_;
        std::unique_ptr<grpc::ServerCompletionQueue> Queue_;
        std::unique_ptr<grpc::Server> Server_;
        grpc::ServerContext Context_;
        Ydb::Query::ExecuteQueryRequest Request_;
        grpc::ServerAsyncWriter<Ydb::Query::ExecuteQueryResponsePart> Writer_;
        TTag Accepted_;
        TTag Metadata_;
        TTag Written_;
        TTag Finished_;
        TTag Done_;
        std::unique_ptr<TDriver> Driver_;
        std::unique_ptr<TQueryClient> Client_;
    };

    void AssertCancelled(TAsyncExecuteQueryPart& read) {
        UNIT_ASSERT(read.Wait(WaitTimeout));
        const auto part = read.ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(part.GetStatus(), EStatus::CLIENT_CANCELLED);
    }

} // namespace

Y_UNIT_TEST_SUITE(QueryStreamCancellation) {
    Y_UNIT_TEST(RequestControlCancelsAfterInitialMetadataAndFirstPart) {
        TQueryStreamServer server;
        auto control = std::make_shared<TRequestControl>();
        auto iterator = server.Start(TExecuteQuerySettings().RequestControl(control));
        auto first = iterator.ReadNext();
        server.WriteSuccess();
        UNIT_ASSERT(first.Wait(WaitTimeout));
        UNIT_ASSERT(first.ExtractValueSync().IsSuccess());

        auto read = iterator.ReadNext();
        UNIT_ASSERT(!read.HasValue());
        control->Cancel();
        control->Cancel();
        AssertCancelled(read);
        server.WaitForCancellation();
        server.Finish();
    }

    Y_UNIT_TEST(CancelBeforeFirstRead) {
        TQueryStreamServer server;
        auto iterator = server.Start();

        iterator.Cancel();
        iterator.Cancel();
        server.WaitForCancellation();
        auto read = iterator.ReadNext();
        AssertCancelled(read);
        iterator.Cancel();
        server.Finish();
    }

    Y_UNIT_TEST(CancelPendingReadAndDestroyIterators) {
        TQueryStreamServer server;
        TAsyncExecuteQueryPart read;
        {
            auto iterator = server.Start();
            auto copy = iterator;
            read = iterator.ReadNext();
            UNIT_ASSERT(!read.HasValue());

            copy.Cancel();
            iterator.Cancel();
        }

        // The pending read owns its buffers and callback even after all handles
        // disappear. Cancellation must not depend on the reader's destructor.
        AssertCancelled(read);
        server.WaitForCancellation();
        server.Finish();
    }

    Y_UNIT_TEST(CancelMovedFromIteratorKeepsStreamOpen) {
        TQueryStreamServer server;
        auto iterator = server.Start();
        auto moved = std::move(iterator);
        iterator.Cancel();
        iterator.Cancel();

        auto first = moved.ReadNext();
        server.WriteSuccess();
        UNIT_ASSERT(first.Wait(WaitTimeout));
        UNIT_ASSERT(first.ExtractValueSync().IsSuccess());

        auto read = moved.ReadNext();
        UNIT_ASSERT(!read.HasValue());
        moved.Cancel();
        AssertCancelled(read);
        server.WaitForCancellation();
        server.Finish();
    }

    Y_UNIT_TEST(ConcurrentCancellationFromCopies) {
        TQueryStreamServer server;
        auto iterator = server.Start();
        auto read = iterator.ReadNext();
        UNIT_ASSERT(!read.HasValue());

        std::thread cancel([copy = iterator]() mutable {
            for (int i = 0; i < 32; ++i) {
                copy.Cancel();
            }
        });
        for (int i = 0; i < 32; ++i) {
            iterator.Cancel();
        }
        cancel.join();

        AssertCancelled(read);
        server.WaitForCancellation();
        server.Finish();
    }

    Y_UNIT_TEST(CancelAfterCompletionKeepsReceivedParts) {
        TQueryStreamServer server;
        auto iterator = server.Start();
        auto read = iterator.ReadNext();
        server.WriteSuccess();
        UNIT_ASSERT(read.Wait(WaitTimeout));
        const auto part = read.ExtractValueSync();
        UNIT_ASSERT(part.IsSuccess());

        auto last = iterator.ReadNext();
        server.Finish();
        UNIT_ASSERT(last.Wait(WaitTimeout));
        const auto eos = last.ExtractValueSync();
        UNIT_ASSERT(eos.EOS());

        iterator.Cancel();
        iterator.Cancel();
        UNIT_ASSERT(part.IsSuccess());
        UNIT_ASSERT(eos.EOS());
    }

    Y_UNIT_TEST(CancelFailedIterator) {
        TDriver driver(TDriverConfig()
                           .SetEndpoint("127.0.0.1:1")
                           .SetDiscoveryMode(EDiscoveryMode::Off)
                           .SetDatabase("/Root"));
        TQueryClient client(driver);
        driver.Stop(true);

        auto stream = client.StreamExecuteQuery("SELECT 1", TTxControl::NoTx());
        UNIT_ASSERT(stream.Wait(WaitTimeout));
        auto iterator = stream.ExtractValueSync();
        UNIT_ASSERT(!iterator.IsSuccess());
        const auto status = iterator.GetStatus();
        iterator.Cancel();
        iterator.Cancel();
        UNIT_ASSERT_VALUES_EQUAL(iterator.GetStatus(), status);
    }
} // Y_UNIT_TEST_SUITE(QueryStreamCancellation)
