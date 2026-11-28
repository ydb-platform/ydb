#include <ydb/library/grpc/server/grpc_request.h>
#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/common/network.h>

#include <util/system/event.h>

#include <memory>
#include <thread>

namespace NYdbGrpc {

// Each injected notification owns a separate Prepare()/DestroyRequest() reference.
// This exercises repeated callbacks, not duplicate delivery of one CQ registration.
struct TGRpcRequestTestAccess {
    template <class TRequest>
    static IQueueEvent* PrepareFinish(TRequest& request) {
        return request.OnFinishTag.Prepare();
    }
};

namespace {

using EFinishStatus = IRequestContextBase::EFinishStatus;
constexpr auto Timeout = TDuration::Seconds(10);

class TDiscoveryService: public TGrpcServiceBase<Ydb::Discovery::V1::DiscoveryService> {
public:
    using TRequest = TGRpcRequestImpl<Ydb::Discovery::ListEndpointsRequest,
        Ydb::Discovery::ListEndpointsResponse, TDiscoveryService,
        NKikimr::TSecurityTextFormatPrinter<Ydb::Discovery::ListEndpointsRequest>,
        NKikimr::TSecurityTextFormatPrinter<Ydb::Discovery::ListEndpointsResponse>>;

    void InitService(grpc::ServerCompletionQueue* cq, TLoggerPtr logger) override {
        MakeIntrusive<TRequest>(this, &Service_, cq,
            [this](IRequestContextBase* request) {
                Received.SetValue(TIntrusivePtr<TRequest>(static_cast<TRequest*>(request)));
            }, &TCurrentGRpcService::AsyncService::RequestListEndpoints,
            "ListEndpoints", logger, FakeCounterBlock(), nullptr)->Run();
    }

    NThreading::TPromise<TIntrusivePtr<TRequest>> Received = NThreading::NewPromise<TIntrusivePtr<TRequest>>();
};

class TRequestFixture {
public:
    TRequestFixture()
        : Service(MakeIntrusive<TDiscoveryService>())
        , Server(TServerOptions().SetHost("127.0.0.1").SetPort(Port)
            .SetWorkerThreads(4).SetWorkersPerCompletionQueue(4))
    {
        Server.AddService(Service);
        Server.Start();
        auto channel = grpc::CreateChannel(TStringBuilder() << "127.0.0.1:" << Server.GetPort(), grpc::InsecureChannelCredentials());
        Stub = Ydb::Discovery::V1::DiscoveryService::NewStub(channel);
        ClientContext.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        Reader = Stub->AsyncListEndpoints(&ClientContext, ClientRequest, &ClientQueue);
        Reader->Finish(&ClientResponse, &ClientStatus, this);
        auto received = Service->Received.GetFuture();
        UNIT_ASSERT(received.Wait(Timeout));
        Request = received.ExtractValue();
    }

    ~TRequestFixture() {
        if (!Replied) {
            Reply();
        }
        Server.Stop();
        ClientQueue.Shutdown();
        void* tag;
        bool ok;
        while (ClientQueue.Next(&tag, &ok)) {
        }
    }

    void Reply() {
        Request->Reply(&ServerResponse, 0);
        Replied = true;
    }

    void Finish() {
        if (!Replied) {
            Reply();
        }
        void* tag = nullptr;
        bool ok = false;
        auto result = ClientQueue.AsyncNext(&tag, &ok,
            std::chrono::system_clock::now() + std::chrono::seconds(10));
        UNIT_ASSERT(result == grpc::CompletionQueue::GOT_EVENT);
        UNIT_ASSERT_VALUES_EQUAL(tag, this);
        UNIT_ASSERT(ok);
        Server.Stop();
    }

    void BeginShutdown() {
        Service->StopService();
    }

    void DeliverFinish(bool ok) {
        auto refs = Request->RefCount();
        auto* tag = TGRpcRequestTestAccess::PrepareFinish(*Request);
        UNIT_ASSERT_VALUES_EQUAL(Request->RefCount(), refs + 1);
        UNIT_ASSERT(!tag->Execute(ok));
        tag->DestroyRequest();
        UNIT_ASSERT_VALUES_EQUAL(Request->RefCount(), refs);
    }

private:
    NTesting::TPortHolder Port = NTesting::GetFreePort();
    TIntrusivePtr<TDiscoveryService> Service;
    TGRpcServer Server;
    grpc::CompletionQueue ClientQueue;
    std::unique_ptr<Ydb::Discovery::V1::DiscoveryService::Stub> Stub;
public:
    grpc::ClientContext ClientContext;

private:
    std::unique_ptr<grpc::ClientAsyncResponseReader<Ydb::Discovery::ListEndpointsResponse>> Reader;
    Ydb::Discovery::ListEndpointsRequest ClientRequest;
    Ydb::Discovery::ListEndpointsResponse ClientResponse;
    Ydb::Discovery::ListEndpointsResponse ServerResponse;
    grpc::Status ClientStatus;
    bool Replied = false;

public:
    TIntrusivePtr<TDiscoveryService::TRequest> Request;
};

struct TFinishObserver {
    struct TState {
        TManualEvent Done;
        std::atomic<unsigned> Calls = 0;
        EFinishStatus Status = EFinishStatus::ERROR;
        bool ClientLost = false;
    };

    explicit TFinishObserver(IRequestContextBase& request) {
        request.GetFinishFuture().Subscribe([state = State, &request](const auto& future) {
            state->Status = future.GetValue();
            state->ClientLost = request.IsClientLost();
            ++state->Calls;
            state->Done.Signal();
        });
    }

    void Check(EFinishStatus expected) {
        UNIT_ASSERT(State->Done.WaitT(Timeout));
        UNIT_ASSERT_VALUES_EQUAL(State->Calls.load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(State->Status, expected);
        UNIT_ASSERT_VALUES_EQUAL(State->ClientLost, expected == EFinishStatus::CANCEL);
    }

    std::shared_ptr<TState> State = std::make_shared<TState>();
};

} // namespace

Y_UNIT_TEST_SUITE(GrpcRequestFinish) {
    Y_UNIT_TEST(NormalListEndpoints) {
        TRequestFixture fixture;
        TFinishObserver observer(*fixture.Request);
        fixture.Finish();
        observer.Check(EFinishStatus::OK);
        UNIT_ASSERT(!fixture.Request->IsClientLost());
    }

    Y_UNIT_TEST(RepeatedAndReentrantNotification) {
        for (bool firstOk : {false, true}) {
            TRequestFixture fixture;
            TFinishObserver observer(*fixture.Request);
            unsigned reentrantCalls = 0;
            fixture.Request->GetFinishFuture().Subscribe([&](const auto&) {
                ++reentrantCalls;
                fixture.DeliverFinish(!firstOk);
            });

            fixture.DeliverFinish(firstOk);
            fixture.DeliverFinish(!firstOk);
            auto expected = firstOk ? EFinishStatus::OK : EFinishStatus::ERROR;
            observer.Check(expected);
            UNIT_ASSERT_VALUES_EQUAL(reentrantCalls, 1);
            UNIT_ASSERT(!fixture.Request->IsClientLost());

            fixture.Finish();
            observer.Check(expected);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Request->GetFinishFuture().GetValue(), expected);
        }
    }

    Y_UNIT_TEST(ClientCancellation) {
        TRequestFixture fixture;
        TFinishObserver observer(*fixture.Request);
        fixture.ClientContext.TryCancel();
        observer.Check(EFinishStatus::CANCEL);
        fixture.Finish();
        fixture.DeliverFinish(true);
        fixture.DeliverFinish(false);
        observer.Check(EFinishStatus::CANCEL);
        UNIT_ASSERT(fixture.Request->IsClientLost());
    }

    Y_UNIT_TEST(CancellationCannotChangeFirstResult) {
        for (bool firstOk : {false, true}) {
            TRequestFixture fixture;
            TFinishObserver observer(*fixture.Request);
            fixture.DeliverFinish(firstOk);
            fixture.ClientContext.TryCancel();
            fixture.Finish();
            auto expected = firstOk ? EFinishStatus::OK : EFinishStatus::ERROR;
            observer.Check(expected);
            UNIT_ASSERT(!fixture.Request->IsClientLost());
        }
    }

    Y_UNIT_TEST(ShutdownPendingRequest) {
        TRequestFixture fixture;
        TFinishObserver observer(*fixture.Request);
        fixture.BeginShutdown();
        observer.Check(EFinishStatus::CANCEL);
        fixture.Finish();
        observer.Check(EFinishStatus::CANCEL);
        UNIT_ASSERT(fixture.Request->IsClientLost());
    }

    Y_UNIT_TEST(ReplyRacesWithCancellation) {
        for (unsigned iteration = 0; iteration < 8; ++iteration) {
            TRequestFixture fixture;
            TFinishObserver observer(*fixture.Request);
            TManualEvent start;
            std::thread reply([&] {
                start.WaitI();
                fixture.Reply();
            });
            std::thread cancel([&] {
                start.WaitI();
                fixture.ClientContext.TryCancel();
            });
            start.Signal();
            reply.join();
            cancel.join();
            fixture.Finish();
            auto status = fixture.Request->GetFinishFuture().GetValue();
            UNIT_ASSERT(status == EFinishStatus::OK || status == EFinishStatus::CANCEL);
            observer.Check(status);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Request->IsClientLost(), status == EFinishStatus::CANCEL);
        }
    }

    Y_UNIT_TEST(ConcurrentNotifications) {
        TRequestFixture fixture;
        TFinishObserver observer(*fixture.Request);
        auto refs = fixture.Request->RefCount();
        auto* first = TGRpcRequestTestAccess::PrepareFinish(*fixture.Request);
        auto* second = TGRpcRequestTestAccess::PrepareFinish(*fixture.Request);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Request->RefCount(), refs + 2);
        TManualEvent start;
        auto deliver = [&start](IQueueEvent* tag, bool ok) {
            start.WaitI();
            Y_ABORT_UNLESS(!tag->Execute(ok));
            tag->DestroyRequest();
        };
        std::thread firstThread(deliver, first, true);
        std::thread secondThread(deliver, second, false);
        start.Signal();
        firstThread.join();
        secondThread.join();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Request->RefCount(), refs);
        auto status = fixture.Request->GetFinishFuture().GetValue();
        UNIT_ASSERT(status == EFinishStatus::OK || status == EFinishStatus::ERROR);
        observer.Check(status);
        UNIT_ASSERT(!fixture.Request->IsClientLost());
        fixture.Finish();
        observer.Check(status);
    }
}

} // namespace NYdbGrpc
