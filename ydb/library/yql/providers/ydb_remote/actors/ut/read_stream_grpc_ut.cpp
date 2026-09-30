#include <ydb/library/yql/providers/ydb_remote/actors/read_stream.h>
#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <arrow/api.h>
#include <arrow/ipc/writer.h>
#include <grpcpp/grpcpp.h>

#include <util/string/builder.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <vector>

namespace NYql::NYdbRemote {
namespace {

constexpr auto WaitTimeout = TDuration::Seconds(10);
using namespace NNative;
using namespace NDq;

TSource Source() {
    TSource source;
    source.SetVersion(1);
    source.SetEndpoint("unused");
    source.SetDatabase("/Remote");
    source.SetTable("/Remote/table");
    auto* column = source.AddColumns();
    column->SetName("value");
    column->MutableType()->set_type_id(Ydb::Type::UINT64);
    return source;
}

Ydb::Query::ExecuteQueryResponsePart Batch() {
    arrow::UInt64Builder builder;
    UNIT_ASSERT(builder.Append(42).ok());
    auto array = builder.Finish().ValueOrDie();
    auto schema = arrow::schema({arrow::field("value", arrow::uint64())});
    auto batch = arrow::RecordBatch::Make(schema, 1, {array});
    Ydb::Query::ExecuteQueryResponsePart part;
    part.set_status(Ydb::StatusIds::SUCCESS);
    auto* result = part.mutable_result_set();
    result->set_format(Ydb::ResultSet::FORMAT_ARROW);
    result->mutable_arrow_format_meta()->set_schema(arrow::ipc::SerializeSchema(*schema).ValueOrDie()->ToString());
    result->set_data(arrow::ipc::SerializeRecordBatch(*batch, arrow::ipc::IpcWriteOptions::Defaults()).ValueOrDie()->ToString());
    return part;
}

class TDeferredCredentialsFactory final : public NYdb::ICredentialsProviderFactory {
    class TProvider final : public NYdb::ICredentialsProvider {
    public:
        std::string GetAuthInfo() const override {
            UNIT_ASSERT(Ready.HasValue());
            return Ready.GetValue();
        }

        NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
            Requested.TrySetValue();
            return Ready.GetFuture();
        }

        bool IsValid() const override { return true; }

        NThreading::TPromise<std::string> Ready = NThreading::NewPromise<std::string>();
        mutable NThreading::TPromise<void> Requested = NThreading::NewPromise<void>();
    };

public:
    NYdb::TCredentialsProviderPtr CreateProvider() const override { return Provider_; }

    void WaitUntilRequested() const {
        UNIT_ASSERT(Provider_->Requested.GetFuture().Wait(WaitTimeout));
    }

    void SetReady() { Provider_->Ready.SetValue("test-token"); }

private:
    const std::shared_ptr<TProvider> Provider_ = std::make_shared<TProvider>();
};

// Each RPC is controlled from the test's completion queue. In particular, an
// accepted RPC can withhold even initial metadata until cancellation arrives.
class TQueryServer {
    struct TTag {
        bool Complete = false;
        bool Ok = false;
    };

public:
    struct TCall {
        TCall() : Writer(&Context) {}
        grpc::ServerContext Context;
        Ydb::Query::ExecuteQueryRequest Request;
        grpc::ServerAsyncWriter<Ydb::Query::ExecuteQueryResponsePart> Writer;
        TTag Accepted, Metadata, Written, Finished, Done;
    };

    TQueryServer() {
        NTesting::InitPortManagerFromEnv();
        const auto endpoint = TStringBuilder() << "127.0.0.1:" << NTesting::GetFreePort();
        grpc::ServerBuilder builder;
        builder.AddListeningPort(endpoint, grpc::InsecureServerCredentials());
        builder.RegisterService(&Service_);
        Queue_ = builder.AddCompletionQueue();
        Server_ = builder.BuildAndStart();
        UNIT_ASSERT(Server_);
        auto config = NYdb::TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Remote")
            .SetDiscoveryMode(NYdb::EDiscoveryMode::Off).SetNetworkThreadsNum(1)
            .SetMaxInboundMessageSize(MaxInboundMessageBytes);
        Driver_ = std::make_unique<NYdb::TDriver>(config);
        TlsDriver_ = std::make_unique<NYdb::TDriver>(config);
        Client = CreateClient(false);
    }

    ~TQueryServer() {
        Server_->Shutdown(std::chrono::system_clock::now());
        Queue_->Shutdown();
        void* tag = nullptr;
        bool ok = false;
        while (Queue_->Next(&tag, &ok)) {}
        Client.reset();
        Driver_->Stop(true);
        TlsDriver_->Stop(true);
    }

    TCall& ExpectCall() {
        auto& call = *Calls_.emplace_back(std::make_unique<TCall>());
        call.Context.AsyncNotifyWhenDone(&call.Done);
        Service_.RequestExecuteQuery(&call.Context, &call.Request, &call.Writer,
            Queue_.get(), Queue_.get(), &call.Accepted);
        return call;
    }

    void Accept(TCall& call) {
        WaitFor(call.Accepted);
        UNIT_ASSERT(call.Accepted.Ok);
    }

    void Open(TCall& call) {
        Accept(call);
        call.Writer.SendInitialMetadata(&call.Metadata);
        WaitFor(call.Metadata);
        UNIT_ASSERT(call.Metadata.Ok);
    }

    void Write(TCall& call, const Ydb::Query::ExecuteQueryResponsePart& part, bool requireSuccess = true) {
        call.Written = {};
        call.Writer.Write(part, &call.Written);
        WaitFor(call.Written);
        UNIT_ASSERT(!requireSuccess || call.Written.Ok);
    }

    void Cancelled(TCall& call) {
        WaitFor(call.Done);
        UNIT_ASSERT(call.Context.IsCancelled());
    }

    void Finish(TCall& call) {
        call.Writer.Finish(grpc::Status::OK, &call.Finished);
        WaitFor(call.Finished);
        WaitFor(call.Done);
    }

    bool AcceptedWithin(TCall& call, TDuration timeout) {
        return WaitFor(call.Accepted, timeout, false);
    }

    std::shared_ptr<NYdb::NQuery::TQueryClient> CreateClient(bool useTls,
            NYdb::TCredentialsProviderFactoryPtr credentials = {}) {
        auto settings = NYdb::NQuery::TClientSettings().DiscoveryMode(NYdb::EDiscoveryMode::Off)
            .SslCredentials(NYdb::TSslCredentials(useTls));
        if (credentials) {
            settings.CredentialsProviderFactory(std::move(credentials));
        }
        return std::make_shared<NYdb::NQuery::TQueryClient>(useTls ? *TlsDriver_ : *Driver_,
            settings);
    }

    std::shared_ptr<NYdb::NQuery::TQueryClient> Client;

private:
    bool WaitFor(TTag& expected, TDuration timeout = WaitTimeout, bool required = true) {
        const auto deadline = std::chrono::system_clock::now() + std::chrono::microseconds(timeout.MicroSeconds());
        while (!expected.Complete) {
            void* tag = nullptr;
            bool ok = false;
            auto status = Queue_->AsyncNext(&tag, &ok, deadline);
            if (status != grpc::CompletionQueue::GOT_EVENT) {
                UNIT_ASSERT(!required);
                return false;
            }
            auto& received = *static_cast<TTag*>(tag);
            UNIT_ASSERT(!received.Complete);
            received.Complete = true;
            received.Ok = ok;
        }
        return true;
    }

    Ydb::Query::V1::QueryService::AsyncService Service_;
    std::unique_ptr<grpc::ServerCompletionQueue> Queue_;
    std::unique_ptr<grpc::Server> Server_;
    std::vector<std::unique_ptr<TCall>> Calls_;
    std::unique_ptr<NYdb::TDriver> Driver_;
    std::unique_ptr<NYdb::TDriver> TlsDriver_;
};

TReadContext Context(TDuration timeout = TDuration::Seconds(30)) {
    TReadContext context;
    context.Deadline = TInstant::Now() + timeout;
    context.MaxBatchBytes = 1024 * 1024;
    return context;
}

void AssertError(NThreading::TFuture<TReadResult>& result) {
    UNIT_ASSERT(result.Wait(WaitTimeout));
    const auto& value = result.GetValue();
    UNIT_ASSERT(value.Error);
    UNIT_ASSERT(!value.Finished);
    UNIT_ASSERT(!value.Batch);
}

class TQuota final : public IMemoryQuotaManager {
public:
    bool AllocateQuota(ui64 bytes, bool optional) override {
        UNIT_ASSERT(optional);
        Allocated += bytes;
        return true;
    }
    void FreeQuota(ui64 bytes) override { Allocated -= bytes; }
    ui64 GetCurrentQuota() const override { return Allocated; }
    ui64 GetMaxMemorySize() const override { return Allocated; }
    i64 GetMemoryAvailability() const override { return ReadMemoryReservation; }
    TString MemoryConsumptionDetails() const override { return {}; }
    std::atomic<ui64> Allocated = 0;
};

// Count public Next invocations while using the real SDK below the adapter.
class TCountingStream final : public IReadStream {
public:
    TCountingStream(std::shared_ptr<IReadStream> stream, std::atomic<ui32>& reads)
        : Stream_(std::move(stream)), Reads_(reads) {}
    NThreading::TFuture<TReadResult> Next() override {
        ++Reads_;
        return Stream_->Next();
    }
    void Cancel() override { Stream_->Cancel(); }
private:
    std::shared_ptr<IReadStream> Stream_;
    std::atomic<ui32>& Reads_;
};

class TActorFixture {
public:
    TActorFixture() {
        Setup.Execute([&](TFakeActor& actor) {
            NDqProto::TTaskInput input;
            THashMap<TString, TString> params;
            TVector<TString> ranges;
            auto [asyncInput, readActor] = CreateNativeReadActor([this](const auto& context) {
                UNIT_ASSERT_VALUES_EQUAL(context.Deadline, Deadline);
                ++Attempts;
                return std::make_shared<TCountingStream>(CreateReadStream(Server.Client, Source(), context), Reads);
            }, {.Timeout = TDuration::Seconds(60), .MaxBatchBytes = 1024 * 1024,
                .MemoryReservation = ReadMemoryReservation, .MaxRetries = 2, .Columns = {"value"}},
            IDqAsyncIoFactory::TSourceArguments{
                .InputDesc = input, .InputIndex = 0, .StatsLevel = {}, .TxId = {}, .TaskId = 1,
                .SecureParams = params, .TaskParams = params, .ReadRanges = ranges,
                .ComputeActorId = actor.SelfId(), .TypeEnv = actor.TypeEnv,
                .HolderFactory = actor.HolderFactory, .ProgramBuilder = actor.ProgramBuilder,
                .MemoryQuotaManager = Quota, .Deadline = Deadline,
            });
            actor.InitAsyncInput(asyncInput, readActor);
        });
        Setup.Execute([](TFakeActor&) {});
    }

    ~TActorFixture() { Setup.Terminate(); }

    NThreading::TFuture<void> Pull(i64 freeSpace, ui64 expectedRows = 0) {
        NThreading::TFuture<void> notification;
        Setup.Execute([&](TFakeActor& actor) {
            NKikimr::NMiniKQL::TUnboxedValueBatch batch;
            TMaybe<TInstant> watermark;
            bool finished = false;
            actor.DqAsyncInput->GetAsyncInputData(batch, watermark, finished, freeSpace);
            UNIT_ASSERT_VALUES_EQUAL(batch.RowCount(), expectedRows);
            notification = Setup.AsyncInputPromises->NewAsyncInputDataArrived.GetFuture();
        });
        return notification;
    }

    // Destruction order keeps the actor system alive until the SDK driver stops.
    TFakeCASetup Setup;
    TQueryServer Server;
    const TInstant Deadline = TInstant::Now() + TDuration::Seconds(30);
    const std::shared_ptr<TQuota> Quota = std::make_shared<TQuota>();
    std::atomic<ui32> Attempts = 0;
    std::atomic<ui32> Reads = 0;
};

} // namespace

Y_UNIT_TEST_SUITE(YdbRemoteReadTransport) {
    Y_UNIT_TEST(IndependentDriversCannotReusePlaintextChannelForTlsClient) {
        TQueryServer server;
        auto readPlain = [&](TQueryServer::TCall& call) {
            auto stream = CreateReadStream(server.CreateClient(false), Source(), Context());
            auto result = stream->Next();
            server.Open(call);
            server.Write(call, Batch());
            UNIT_ASSERT(result.Wait(WaitTimeout));
            UNIT_ASSERT(!result.GetValue().Error);
            UNIT_ASSERT(result.GetValue().Batch);
            UNIT_ASSERT_VALUES_EQUAL(result.GetValue().Batch->num_rows(), 1);
            stream->Cancel();
            server.Cancelled(call);
            server.Finish(call);
        };
        auto& first = server.ExpectCall();
        readPlain(first);

        // This accept remains pending across the TLS handshake failure and is
        // consumed only by the subsequent explicitly plaintext client.
        auto& next = server.ExpectCall();
        auto tls = CreateReadStream(server.CreateClient(true), Source(), Context(TDuration::Seconds(2)));
        auto result = tls->Next();
        AssertError(result);
        tls->Cancel();
        tls.reset();
        UNIT_ASSERT(!server.AcceptedWithin(next, TDuration::MilliSeconds(100)));
        readPlain(next);
    }

    Y_UNIT_TEST(CancelUnstartedStreamDoesNotOpenRpc) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto stream = CreateReadStream(server.Client, Source(), Context());
        stream->Cancel();
        auto result = stream->Next();
        AssertError(result);
        UNIT_ASSERT(!server.AcceptedWithin(call, TDuration::MilliSeconds(100)));
    }

    Y_UNIT_TEST(ConcurrentPullDoesNotReplacePendingRead) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto stream = CreateReadStream(server.Client, Source(), Context());
        auto first = stream->Next();
        server.Open(call);
        auto concurrent = stream->Next();
        UNIT_ASSERT(concurrent.HasValue());
        AssertError(concurrent);
        UNIT_ASSERT(!first.HasValue());
        server.Write(call, Batch());
        UNIT_ASSERT(first.Wait(WaitTimeout));
        UNIT_ASSERT(!first.GetValue().Error);
        UNIT_ASSERT(first.GetValue().Batch);
        stream->Cancel();
        server.Cancelled(call);
        server.Finish(call);
    }

    Y_UNIT_TEST(CancelPendingInitialOpenCompletesLocallyAndDiscardsLateIterator) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto credentials = std::make_shared<TDeferredCredentialsFactory>();
        auto released = NThreading::NewPromise();
        auto context = Context();
        context.MemoryLease = std::shared_ptr<void>(new int, [released](void* value) mutable {
            delete static_cast<int*>(value);
            released.SetValue();
        });
        auto stream = CreateReadStream(server.CreateClient(false, credentials), Source(), context);
        context.MemoryLease.reset();
        auto result = stream->Next();
        credentials->WaitUntilRequested();
        UNIT_ASSERT(!result.HasValue());
        UNIT_ASSERT(!server.AcceptedWithin(call, TDuration::MilliSeconds(100)));
        stream->Cancel();
        UNIT_ASSERT(result.HasValue());
        AssertError(result);
        stream.reset();
        UNIT_ASSERT(!released.HasValue());
        // The SDK creates its iterator at RPC start, before initial metadata.
        // Pending credentials hold that start deterministically. Releasing them
        // after local cancellation must discard the late iterator without ReadNext.
        credentials->SetReady();
        // No server response was sent. The opening callback releases the provider
        // reservation even while the cancelled result future remains alive.
        UNIT_ASSERT(released.GetFuture().Wait(WaitTimeout));
        // Reader destruction can cancel the RPC before the server dispatches it.
        // If it was dispatched, cancellation must also reach the server.
        if (server.AcceptedWithin(call, TDuration::MilliSeconds(100))) {
            server.Cancelled(call);
            server.Finish(call);
        }
        AssertError(result);
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().Error, "YdbRemote read cancelled");
    }

    Y_UNIT_TEST(CancelPendingReadCompletesLocallyAndDiscardsLateBatch) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        NThreading::TCancellationTokenSource cancellation;
        auto context = Context();
        context.Cancellation = cancellation.Token();
        auto stream = CreateReadStream(server.Client, Source(), context);
        auto first = stream->Next();
        server.Open(call);
        server.Write(call, Batch());
        UNIT_ASSERT(first.Wait(WaitTimeout));
        UNIT_ASSERT(!first.GetValue().Error);
        UNIT_ASSERT(first.GetValue().Batch);
        first = {};
        // The iterator is established and ReadNext is now definitely in flight.
        auto result = stream->Next();
        UNIT_ASSERT(!result.HasValue());
        cancellation.Cancel();
        UNIT_ASSERT(result.HasValue());
        AssertError(result);
        server.Write(call, Batch(), false);
        server.Cancelled(call);
        server.Finish(call);
        AssertError(result);
        auto next = stream->Next();
        AssertError(next);
    }

    Y_UNIT_TEST(CancelledProviderRetainsLeaseUntilPendingSdkCallbackReturns) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto released = NThreading::NewPromise();
        auto context = Context();
        context.MemoryLease = std::shared_ptr<void>(new int, [released](void* value) mutable {
            delete static_cast<int*>(value);
            released.SetValue();
        });
        auto stream = CreateReadStream(server.Client, Source(), context);
        context.MemoryLease.reset();
        auto first = stream->Next();
        server.Open(call);
        server.Write(call, Batch());
        UNIT_ASSERT(first.Wait(WaitTimeout));
        UNIT_ASSERT(!first.GetValue().Error);
        UNIT_ASSERT(first.GetValue().Batch);
        first = {};
        // The iterator is established and ReadNext is now definitely in flight.
        auto result = stream->Next();
        UNIT_ASSERT(!released.HasValue());
        stream->Cancel();
        UNIT_ASSERT(result.HasValue());
        stream.reset();
        AssertError(result);
        UNIT_ASSERT(!released.HasValue());
        // The result future stays alive: only the late SDK callback should own
        // the stream/lease now. This does not assert SDK-internal quiescence.
        server.Write(call, Batch(), false);
        server.Cancelled(call);
        UNIT_ASSERT(released.GetFuture().Wait(WaitTimeout));
        server.Finish(call);
        AssertError(result);
    }

    Y_UNIT_TEST(AbsoluteDeadlineCancelsPendingInitialOpen) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto stream = CreateReadStream(server.Client, Source(), Context(TDuration::Seconds(1)));
        auto result = stream->Next();
        server.Accept(call);
        AssertError(result);
        server.Cancelled(call);
        server.Finish(call);
    }

    Y_UNIT_TEST(AbsoluteDeadlineCancelsPendingRead) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto stream = CreateReadStream(server.Client, Source(), Context(TDuration::Seconds(1)));
        auto first = stream->Next();
        server.Open(call);
        server.Write(call, Batch());
        UNIT_ASSERT(first.Wait(WaitTimeout));
        UNIT_ASSERT(!first.GetValue().Error);
        UNIT_ASSERT(first.GetValue().Batch);
        first = {};
        // The iterator is established and ReadNext is now definitely in flight.
        auto result = stream->Next();
        AssertError(result);
        server.Cancelled(call);
        server.Finish(call);
    }

    Y_UNIT_TEST(RejectsOversizedWirePart) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto stream = CreateReadStream(server.Client, Source(), Context());
        auto result = stream->Next();
        server.Open(call);
        auto part = Batch();
        part.mutable_result_set()->set_data(std::string(MaxInboundMessageBytes + 1, 'x'));
        server.Write(call, part, false); // The receiver can reject its advertised size before Write completes.
        AssertError(result);
        stream->Cancel();
        server.Cancelled(call);
        server.Finish(call);
    }

    Y_UNIT_TEST(CompressedResponseIsValidatedAfterSdkDecode) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto stream = CreateReadStream(server.Client, Source(), Context());
        auto result = stream->Next();
        server.Accept(call);
        // The baseline transport accepts compression. Provider Arrow validation
        // runs only after gRPC decompression and SDK protobuf decoding.
        call.Context.set_compression_algorithm(GRPC_COMPRESS_GZIP);
        server.Write(call, Batch());
        UNIT_ASSERT(result.Wait(WaitTimeout));
        UNIT_ASSERT(!result.GetValue().Error);
        UNIT_ASSERT(result.GetValue().Batch);
        UNIT_ASSERT_VALUES_EQUAL(result.GetValue().Batch->num_rows(), 1);
        stream->Cancel();
        server.Cancelled(call);
        server.Finish(call);
    }

    Y_UNIT_TEST(OversizedCompressedResponseFailsAfterSdkProcessing) {
        TQueryServer server;
        auto& call = server.ExpectCall();
        auto released = NThreading::NewPromise();
        auto context = Context();
        context.MemoryLease = std::shared_ptr<void>(new int, [released](void* value) mutable {
            delete static_cast<int*>(value);
            released.SetValue();
        });
        auto stream = CreateReadStream(server.Client, Source(), context);
        context.MemoryLease.reset();
        auto result = stream->Next();
        server.Accept(call);
        call.Context.set_compression_algorithm(GRPC_COMPRESS_GZIP);
        auto part = Batch();
        part.mutable_result_set()->set_data(std::string(2 * MaxInboundMessageBytes, 'x'));
        {
            const auto protobuf = part.SerializeAsString();
            TString gzip;
            TStringOutput output(gzip);
            TZLibCompress compress(&output, ZLib::GZip);
            compress.Write(protobuf.data(), protobuf.size());
            compress.Finish();
            UNIT_ASSERT(protobuf.size() > MaxInboundMessageBytes);
            UNIT_ASSERT(gzip.size() < MaxInboundMessageBytes);
        }
        server.Write(call, part, false);
        AssertError(result);
        stream->Cancel();
        stream.reset();
        result = {};
        server.Cancelled(call);
        UNIT_ASSERT(released.GetFuture().Wait(WaitTimeout));
        server.Finish(call);
    }

    Y_UNIT_TEST(SlowConsumerDoesNotIssueAdditionalReads) {
        TActorFixture fixture;
        auto& call = fixture.Server.ExpectCall();
        fixture.Pull(0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Attempts.load(), 0);
        auto ready = fixture.Pull(1);
        fixture.Server.Open(call);
        fixture.Server.Write(call, Batch());
        UNIT_ASSERT(ready.Wait(WaitTimeout));
        fixture.Pull(0);
        fixture.Pull(-1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Reads.load(), 1);
        fixture.Pull(1, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Reads.load(), 1);
        fixture.Pull(1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Reads.load(), 2);
        fixture.Setup.Terminate();
        // ReadNext is pending: deliver a late part so the baseline SDK can
        // release its reader before the original request deadline.
        fixture.Server.Write(call, Batch(), false);
        fixture.Server.Cancelled(call);
        fixture.Server.Finish(call);
    }

    Y_UNIT_TEST(TransientFailureBeforeDeliveryRetriesWithOriginalDeadline) {
        TActorFixture fixture;
        auto& first = fixture.Server.ExpectCall();
        auto& second = fixture.Server.ExpectCall();
        auto ready = fixture.Pull(1);
        fixture.Server.Open(first);
        Ydb::Query::ExecuteQueryResponsePart error;
        error.set_status(Ydb::StatusIds::UNAVAILABLE);
        fixture.Server.Write(first, error);
        fixture.Server.Cancelled(first);
        fixture.Server.Finish(first);
        fixture.Server.Open(second);
        fixture.Server.Write(second, Batch());
        UNIT_ASSERT(ready.Wait(WaitTimeout));
        fixture.Pull(1, 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Attempts.load(), 2);
        fixture.Setup.Terminate();
        fixture.Server.Cancelled(second);
        fixture.Server.Finish(second);
    }

    Y_UNIT_TEST(TransientFailureAfterDeliveryCannotOpenAnotherSnapshot) {
        TActorFixture fixture;
        auto& first = fixture.Server.ExpectCall();
        auto& unexpected = fixture.Server.ExpectCall();
        auto ready = fixture.Pull(1);
        fixture.Server.Open(first);
        fixture.Server.Write(first, Batch());
        UNIT_ASSERT(ready.Wait(WaitTimeout));
        fixture.Pull(1, 1);
        auto error = fixture.Setup.AsyncInputPromises->FatalError.GetFuture();
        fixture.Pull(1);
        Ydb::Query::ExecuteQueryResponsePart failure;
        failure.set_status(Ydb::StatusIds::UNAVAILABLE);
        fixture.Server.Write(first, failure);
        UNIT_ASSERT(error.Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Attempts.load(), 1);
        fixture.Server.Cancelled(first);
        fixture.Server.Finish(first);
        UNIT_ASSERT(!fixture.Server.AcceptedWithin(unexpected, TDuration::MilliSeconds(300)));
    }
}

} // namespace NYql::NYdbRemote
