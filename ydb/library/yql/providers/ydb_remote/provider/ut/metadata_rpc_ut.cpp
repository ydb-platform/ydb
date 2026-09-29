#include <ydb/library/yql/providers/ydb_remote/provider/yql_ydb_remote_provider_impl.h>
#include <ydb/library/yql/providers/native/operation_context.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>

#include <grpcpp/completion_queue.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/async_unary_call.h>

#include <atomic>
#include <chrono>

namespace NYql::NYdbRemote {
namespace {

constexpr TDuration WaitTimeout = TDuration::Seconds(10);

struct TTag {
    bool Complete = false;
    bool Ok = false;
};

template <typename TRequest, typename TResponse>
struct TUnaryCall {
    grpc::ServerContext Context;
    TRequest Request;
    grpc::ServerAsyncResponseWriter<TResponse> Writer{&Context};
    TTag Accepted;
    TTag Finished;
    TTag Done;
};

class TMetadataServer {
public:
    TMetadataServer() {
        NTesting::InitPortManagerFromEnv();
        Endpoint = TStringBuilder() << "127.0.0.1:" << NTesting::GetFreePort();
        grpc::ServerBuilder builder;
        builder.AddListeningPort(Endpoint, grpc::InsecureServerCredentials());
        builder.RegisterService(&Service_);
        Queue_ = builder.AddCompletionQueue();
        Server_ = builder.BuildAndStart();
        UNIT_ASSERT(Server_);
        Create.Context.AsyncNotifyWhenDone(&Create.Done);
        Describe.Context.AsyncNotifyWhenDone(&Describe.Done);
        Service_.RequestCreateSession(&Create.Context, &Create.Request, &Create.Writer,
            Queue_.get(), Queue_.get(), &Create.Accepted);
        Service_.RequestDescribeTable(&Describe.Context, &Describe.Request, &Describe.Writer,
            Queue_.get(), Queue_.get(), &Describe.Accepted);
    }

    ~TMetadataServer() {
        Server_->Shutdown(std::chrono::system_clock::now());
        Queue_->Shutdown();
        void* tag = nullptr;
        bool ok = false;
        while (Queue_->Next(&tag, &ok)) {
        }
    }

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

    void ReturnSession() {
        WaitFor(Create.Accepted);
        UNIT_ASSERT(Create.Accepted.Ok);
        Ydb::Table::CreateSessionResult result;
        result.set_session_id("native-metadata-session");
        Ydb::Table::CreateSessionResponse response;
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        response.mutable_operation()->mutable_result()->PackFrom(result);
        Create.Writer.Finish(response, grpc::Status::OK, &Create.Finished);
        WaitFor(Create.Finished);
        UNIT_ASSERT(Create.Finished.Ok);
    }

    TString Endpoint;
    TUnaryCall<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse> Create;
    TUnaryCall<Ydb::Table::DescribeTableRequest, Ydb::Table::DescribeTableResponse> Describe;

private:
    Ydb::Table::V1::TableService::AsyncService Service_;
    std::unique_ptr<grpc::ServerCompletionQueue> Queue_;
    std::unique_ptr<grpc::Server> Server_;
};

class TTrackingQuota final : public NNative::IAsyncMemoryQuota {
    struct TCounters {
        std::atomic<ui64> Bytes = 0;
        std::atomic<ui64> ResponseLeases = 0;
        std::atomic<bool> Overlap = false;
        NThreading::TPromise<void> Released = NThreading::NewPromise<void>();
    };

public:
    NThreading::TFuture<std::shared_ptr<void>> Acquire(
        ui64 bytes, TInstant deadline, NThreading::TCancellationToken cancellation) override {
        UNIT_ASSERT(deadline > TInstant::Now());
        cancellation.ThrowIfCancellationRequested();
        Counters->Bytes.fetch_add(bytes);
        if (bytes == MetadataResponseReservation && Counters->ResponseLeases.fetch_add(1)) {
            Counters->Overlap = true;
        }
        auto lease = std::shared_ptr<void>(new int(0), [counters = Counters, bytes](void* value) {
            delete static_cast<int*>(value);
            if (bytes == MetadataResponseReservation) {
                counters->ResponseLeases.fetch_sub(1);
            }
            if (counters->Bytes.fetch_sub(bytes) == bytes) {
                counters->Released.TrySetValue();
            }
        });
        return NThreading::MakeFuture(std::move(lease));
    }

    void Shutdown() override {}

    void WaitForRelease() const {
        UNIT_ASSERT(Counters->Released.GetFuture().Wait(WaitTimeout));
        UNIT_ASSERT_VALUES_EQUAL(Counters->Bytes.load(), 0);
        UNIT_ASSERT(!Counters->Overlap);
    }

private:
    const std::shared_ptr<TCounters> Counters = std::make_shared<TCounters>();
};

void CheckMetadataCancellation(bool cancelDescribe, bool expireDeadline = false) {
    TMetadataServer server;
    NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(server.Endpoint)
        .SetDiscoveryMode(NYdb::EDiscoveryMode::Off).SetDatabase("/Remote")
        .SetNetworkThreadsNum(1).SetClientThreadsNum(1));
    auto quota = std::make_shared<TTrackingQuota>();
    auto types = MakeIntrusive<TTypeAnnotationContext>();
    auto state = MakeIntrusive<TState>(types.Get(), driver, CreateStructuredTokenCredentialsFactory(),
        TInstant::Now() + TDuration::Seconds(expireDeadline ? 5 : 30), quota);
    AddCluster(*state, "remote", {{"location", server.Endpoint}, {"database_name", "/Remote"},
        {"authMethod", "NONE"}, {"use_tls", "false"}});
    TExprContext ctx;
    const auto pos = ctx.AppendPosition({});
    auto input = ctx.NewCallable(pos, "Read!", {
        ctx.NewWorld(pos), ctx.NewCallable(pos, "DataSource", {
            ctx.NewAtom(pos, YdbRemoteProviderName), ctx.NewAtom(pos, "remote")}),
        ctx.NewCallable(pos, "Key", {ctx.NewList(pos, {ctx.NewAtom(pos, "table"),
            ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, "items")})})}),
        ctx.NewCallable(pos, "Void", {}), ctx.NewList(pos, {})});
    auto transformer = CreateLoadMetadataTransformer(state);
    TExprNode::TPtr output;
    UNIT_ASSERT_VALUES_EQUAL(transformer->Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Async);
    auto future = transformer->GetAsyncFuture(*input);
    server.WaitFor(server.Create.Accepted);
    UNIT_ASSERT(server.Create.Accepted.Ok);
    if (cancelDescribe) {
        server.ReturnSession();
        server.WaitFor(server.Describe.Accepted);
        UNIT_ASSERT(server.Describe.Accepted.Ok);
    }
    UNIT_ASSERT(!future.HasValue());
    if (!expireDeadline) {
        transformer->Rewind();
    }
    auto& done = cancelDescribe ? server.Describe.Done : server.Create.Done;
    auto& context = cancelDescribe ? server.Describe.Context : server.Create.Context;
    server.WaitFor(done);
    UNIT_ASSERT(context.IsCancelled());
    UNIT_ASSERT(future.Wait(WaitTimeout));
    if (expireDeadline) {
        UNIT_ASSERT_VALUES_EQUAL(transformer->ApplyAsyncChanges(input, output, ctx).Level, IGraphTransformer::TStatus::Error);
        UNIT_ASSERT(ctx.IssueManager.GetIssues().ToString().Contains("deadline exceeded"));
    }
    quota->WaitForRelease();
    driver.Stop(true);
}

} // namespace

Y_UNIT_TEST_SUITE(TYdbRemoteMetadataRpc) {
    Y_UNIT_TEST(RewindCancelsCreateSessionTransportAndReleasesMemory) {
        CheckMetadataCancellation(false);
    }

    Y_UNIT_TEST(RewindCancelsDescribeTransportAndReleasesMemory) {
        CheckMetadataCancellation(true);
    }

    Y_UNIT_TEST(SharedDeadlineCancelsPendingTransportAndReleasesMemory) {
        CheckMetadataCancellation(false, true);
    }
}

} // namespace NYql::NYdbRemote
