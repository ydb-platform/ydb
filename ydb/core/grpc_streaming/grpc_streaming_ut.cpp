#include "grpc_streaming.h"

#include <ydb/core/grpc_streaming/ut/grpc/streaming_service.grpc.pb.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/registar.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_SERVER

namespace NKikimr {
namespace NGRpcServer {
namespace NTest {

using namespace Tests;

using IContext = IGRpcStreamingContext<
    NStreamingTest::TSessionRequest,
    NStreamingTest::TSessionResponse>;

namespace {
    NStreamingTest::TSessionRequest MakeRequest(ui64 cookie) {
        NStreamingTest::TSessionRequest request;
        request.SetRequestCookie(cookie);
        return request;
    }

    NStreamingTest::TSessionResponse MakeResponse(ui64 cookie) {
        NStreamingTest::TSessionResponse response;
        response.SetResponseCookie(cookie);
        return response;
    }
}

template<class TImplActor>
class TStreamingService : public NYdbGrpc::TGrpcServiceBase<NStreamingTest::TStreamingService> {
public:
    using TSelf = TStreamingService<TImplActor>;

    using TRequest = TGRpcStreamingRequest<
        NStreamingTest::TSessionRequest,
        NStreamingTest::TSessionResponse,
        TSelf,
        413>; //GRPC_SERVER = 413, services.proto

    TStreamingService(TActorSystem* as, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : ActorSystem(*as)
        , Counters(std::move(counters))
    { }

    void InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr) override {
        CQ = cq;

        auto getCounterBlock = NGRpcService::CreateCounterCb(Counters, &ActorSystem);

        auto acceptCallback = [this](TIntrusivePtr<IContext> context) {
            ActorSystem.Register(new TImplActor(std::move(context)));
        };

        TRequest::Start(
            this, this->GetService(), CQ,
            &NStreamingTest::TStreamingService::AsyncService::RequestSession,
            acceptCallback,
            ActorSystem,
            "Session",
            getCounterBlock("streaming", "Session"));
    }

    void SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter*) override {
        // nothing
    }

    bool IncRequest() override {
        return true;
    }

    void DecRequest() override {
        // nothing
    }

private:
    TActorSystem& ActorSystem;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> const Counters;

    grpc::ServerCompletionQueue* CQ = nullptr;
};

template<class TImplActor>
class TGRpcTestServer {
public:
    TGRpcTestServer() {
        NYdbGrpc::TServerOptions options;
        Init(std::move(options));
    }

    explicit TGRpcTestServer(NYdbGrpc::TServerOptions options) {
        Init(std::move(options));
    }

    ~TGRpcTestServer() {
        if (GRpcServer) {
            GRpcServer->Stop();
        }
    }

private:
    void Init(NYdbGrpc::TServerOptions options) {
        ui64 port = PortManager.GetPort(2134);
        ui64 grpc = PortManager.GetPort(2135);
        ServerSettings = new TServerSettings(port);
        ServerSettings->SetDomainName("Root");
        Server.Reset(new TServer(*ServerSettings));

        Server->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);

        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters(MakeIntrusive<::NMonitoring::TDynamicCounters>());

        options.SetPort(grpc);
        GRpcServer.Reset(new NYdbGrpc::TGRpcServer(options));

        auto* as = Server->GetRuntime()->GetAnyNodeActorSystem();

        GRpcServer->AddService(new TStreamingService<TImplActor>(as, counters));
        GRpcServer->Start();

        GRpcEndpoint = TStringBuilder() << "localhost:" << grpc;
    }
public:
    TServerSettings::TPtr ServerSettings;
    // GRpcServer must be declared before Server so that it is destroyed AFTER
    // Server. The TServer destructor stops the actor system, which destroys actors that hold
    // TFacade objects. The TFacade destructor calls TGRpcStreamingRequest::FinishInternal(), which
    // accesses Server->IsShuttingDown(). If GRpcServer were destroyed first,
    // this would be a use-after-free.
    THolder<NYdbGrpc::TGRpcServer> GRpcServer;
    TServer::TPtr Server;
    TString GRpcEndpoint;

private:
    TPortManager PortManager;
};

class TSimpleEchoActor : public TActorBootstrapped<TSimpleEchoActor> {
public:
    TSimpleEchoActor(TIntrusivePtr<IContext> context)
        : Context(std::move(context))
    { }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Context->Attach(SelfId());
        Context->Read();
        Become(&TSimpleEchoActor::StateWork);
    }

    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvReadFinished",
            {"success", ev->Get()->Success});
        Context->Write(MakeResponse(ev->Get()->Record.GetRequestCookie()));
        Context->Finish(grpc::Status::OK);
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IContext::TEvReadFinished, Handle);
        }
    }

private:
    TIntrusivePtr<IContext> Context;
};

class TReadFailActor : public TActorBootstrapped<TReadFailActor> {
public:
    TReadFailActor(TIntrusivePtr<IContext> context)
        : Context(std::move(context))
    { }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Context->Attach(SelfId());
        Context->Read();
        Context->Write(MakeResponse(42));
        Context->Finish(grpc::Status::OK);
        Become(&TReadFailActor::StateWork);
    }

    void Step() {
        if (++Counter == 3) {
            ActorFinished.Signal();
            PassAway();
        }
    }

    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvReadFinished",
            {"success", ev->Get()->Success});
        Y_ABORT_UNLESS(!ev->Get()->Success, "Unexpected read success");
        Step();
    }

    void Handle(IContext::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvWriteFinished",
            {"success", ev->Get()->Success});
        Y_ABORT_UNLESS(ev->Get()->Success, "Unexpected write failure");
        Step();
    }

    void Handle(IContext::TEvNotifiedWhenDone::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvNotifiedWhenDone");
        Step();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(IContext::TEvWriteFinished, Handle);
            HFunc(IContext::TEvNotifiedWhenDone, Handle);
        }
    }

public:
    static TManualEvent ActorFinished;

private:
    TIntrusivePtr<IContext> Context;
    ui64 Counter = 0;
};

TManualEvent TReadFailActor::ActorFinished;

class TDisconnectWaitActor : public TActorBootstrapped<TDisconnectWaitActor> {
public:
    TDisconnectWaitActor(TIntrusivePtr<IContext> context)
        : Context(std::move(context))
    { }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Context->Attach(SelfId());
        Become(&TDisconnectWaitActor::StateWork);
    }

    void Handle(IContext::TEvNotifiedWhenDone::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvNotifiedWhenDone");
        ActorFinished.Signal();
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IContext::TEvNotifiedWhenDone, Handle);
        }
    }

public:
    static TManualEvent ActorFinished;

private:
    TIntrusivePtr<IContext> Context;
};

TManualEvent TDisconnectWaitActor::ActorFinished;

class TReadFinishActor : public TActorBootstrapped<TReadFinishActor> {
public:
    TReadFinishActor(TIntrusivePtr<IContext> context)
        : Context(std::move(context))
    { }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Context->Attach(SelfId());
        Context->Read();
        Context->Finish(grpc::Status::OK);
        Become(&TReadFinishActor::StateWork);
    }

    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvReadFinished",
            {"success", ev->Get()->Success});
        if (++Counter == 1) {
            ActorFinished.Signal();
            PassAway();
        }
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IContext::TEvReadFinished, Handle);
        }
    }

public:
    static TManualEvent ActorFinished;

private:
    TIntrusivePtr<IContext> Context;
    ui64 Counter = 0;
};

TManualEvent TReadFinishActor::ActorFinished;

class TExpectWritesDoneActor : public TActorBootstrapped<TExpectWritesDoneActor> {
public:
    TExpectWritesDoneActor(TIntrusivePtr<IContext> context)
        : Context(std::move(context))
    { }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Context->Attach(SelfId());
        Context->Read();
        Become(&TExpectWritesDoneActor::StateWork);
    }

    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvReadFinished",
            {"success", ev->Get()->Success});
        Y_ABORT_UNLESS(ev->Get()->Success == false, "Unexpected Read success");
        ReadFinished.Signal();

        // It should be possible to reply with an OK status here
        Context->Finish(grpc::Status(grpc::StatusCode::FAILED_PRECONDITION, "Everything is A-OK"));
    }

    void Handle(IContext::TEvNotifiedWhenDone::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvNotifiedWhenDone");
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(IContext::TEvNotifiedWhenDone, Handle);
        }
    }

public:
    static TManualEvent ReadFinished;

private:
    TIntrusivePtr<IContext> Context;
};

TManualEvent TExpectWritesDoneActor::ReadFinished;

class TWriteAndFinishActor : public TActorBootstrapped<TWriteAndFinishActor> {
public:
    TWriteAndFinishActor(TIntrusivePtr<IContext> context)
        : Context(std::move(context))
    { }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Context->Attach(SelfId());
        Context->Write(MakeResponse(123));
        Context->WriteAndFinish(MakeResponse(456), grpc::Status::OK);
        Become(&TWriteAndFinishActor::StateWork);
    }

    void Handle(IContext::TEvWriteFinished::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_DEBUG_CTX(ctx, "Received TEvWriteFinished",
            {"success", ev->Get()->Success});
        if (++Counter == 2) {
            PassAway();
        }
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
            HFunc(IContext::TEvWriteFinished, Handle);
        }
    }

private:
    TIntrusivePtr<IContext> Context;
    ui64 Counter = 0;
};

// Actor that attaches and reads but never finishes/closes the stream.
// It keeps the IStreamCtx (TFacade) alive until the actor is destroyed,
// which reproduces the LOGBROKER-10618 crash: when the gRPC server is
// stopped (CQ shutdown) before the actor system is destroyed, the facade
// destructor calls Detach() -> FinishInternal() -> Stream.Finish() on a
// dead completion queue.
class THangActor : public TActorBootstrapped<THangActor> {
public:
    THangActor(TIntrusivePtr<IContext> context)
        : Context(std::move(context))
    { }

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        Context->Attach(SelfId());
        Context->Read();
        // Intentionally do NOT write: a pending write would set FlagWriteActive,
        // which makes FinishInternal() skip Stream.Finish() (finish == false).
        // The LOGBROKER-10618 crash is Stream.Finish() on a dead CQ, which only
        // happens when FlagWriteActive is clear (e.g. a deferred StreamWrite that
        // never started writing). So keep the write side idle to reproduce it.
        if (Attached) {
            Attached->Signal();
        }
        Become(&THangActor::StateWork);
    }

    void Handle(IContext::TEvReadFinished::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
        // Intentionally do nothing: keep the stream open and the context alive.
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(IContext::TEvReadFinished, Handle);
        }
    }

public:
    static TManualEvent* Attached;

private:
    TIntrusivePtr<IContext> Context;
};

TManualEvent* THangActor::Attached = nullptr;

Y_UNIT_TEST_SUITE(TGRpcStreamingTest) {
    Y_UNIT_TEST(SimpleEcho) {
        auto server = MakeHolder<TGRpcTestServer<TSimpleEchoActor>>();

        auto channel = grpc::CreateChannel(server->GRpcEndpoint, grpc::InsecureChannelCredentials());
        auto stub = NStreamingTest::TStreamingService::NewStub(channel);

        grpc::ClientContext context;
        auto stream = stub->Session(&context);

        bool writeOk = stream->Write(MakeRequest(123));
        UNIT_ASSERT_VALUES_EQUAL(writeOk, true);

        NStreamingTest::TSessionResponse response;
        bool readOk = stream->Read(&response);
        UNIT_ASSERT_VALUES_EQUAL(readOk, true);
        UNIT_ASSERT_VALUES_EQUAL(response.GetResponseCookie(), 123u);

        auto status = stream->Finish();
        UNIT_ASSERT(status.ok());
    }

    Y_UNIT_TEST(ClientNeverWrites) {
        auto server = MakeHolder<TGRpcTestServer<TReadFailActor>>();

        auto channel = grpc::CreateChannel(server->GRpcEndpoint, grpc::InsecureChannelCredentials());
        auto stub = NStreamingTest::TStreamingService::NewStub(channel);

        grpc::ClientContext context;
        auto stream = stub->Session(&context);

        // Server actor should stop after sending us a reply
        // Getting stuck on read until client is destroyed would be a bug
        TReadFailActor::ActorFinished.WaitI();

        NStreamingTest::TSessionResponse response;
        bool readOk = stream->Read(&response);
        UNIT_ASSERT_VALUES_EQUAL(readOk, true);
        UNIT_ASSERT_VALUES_EQUAL(response.GetResponseCookie(), 42u);

        auto status = stream->Finish();
        UNIT_ASSERT(status.ok());
    }

    Y_UNIT_TEST(ClientDisconnects) {
        auto server = MakeHolder<TGRpcTestServer<TDisconnectWaitActor>>();

        auto channel = grpc::CreateChannel(server->GRpcEndpoint, grpc::InsecureChannelCredentials());
        auto stub = NStreamingTest::TStreamingService::NewStub(channel);

        {
            grpc::ClientContext context;
            auto stream = stub->Session(&context);
        }

        // Server actor should be notified when client is destroyed
        // Getting stuck indefinitely waiting would be a bug
        TDisconnectWaitActor::ActorFinished.WaitI();
    }

    Y_UNIT_TEST(ReadFinish) {
        auto server = MakeHolder<TGRpcTestServer<TReadFinishActor>>();

        auto channel = grpc::CreateChannel(server->GRpcEndpoint, grpc::InsecureChannelCredentials());
        auto stub = NStreamingTest::TStreamingService::NewStub(channel);

        grpc::ClientContext context;
        auto stream = stub->Session(&context);

        // Getting stuck on reads or anything else would be a bug
        TReadFinishActor::ActorFinished.WaitI();

        auto status = stream->Finish();
        UNIT_ASSERT(status.ok());
    }

    Y_UNIT_TEST(WritesDoneFromClient) {
        auto server = MakeHolder<TGRpcTestServer<TExpectWritesDoneActor>>();

        auto channel = grpc::CreateChannel(server->GRpcEndpoint, grpc::InsecureChannelCredentials());
        auto stub = NStreamingTest::TStreamingService::NewStub(channel);

        grpc::ClientContext context;
        auto stream = stub->Session(&context);
        stream->WritesDone();

        // Getting stuck on read would be a bug (client closed its write side)
        TExpectWritesDoneActor::ReadFinished.WaitI();

        // Read failed on the server, but it's OK reply should reach us
        auto status = stream->Finish();
        UNIT_ASSERT(status.error_code() == grpc::StatusCode::FAILED_PRECONDITION);
        UNIT_ASSERT_VALUES_EQUAL(status.error_message(), "Everything is A-OK");
    }

    Y_UNIT_TEST(WriteAndFinishWorks) {
        auto server = MakeHolder<TGRpcTestServer<TWriteAndFinishActor>>();

        auto channel = grpc::CreateChannel(server->GRpcEndpoint, grpc::InsecureChannelCredentials());
        auto stub = NStreamingTest::TStreamingService::NewStub(channel);

        grpc::ClientContext context;
        auto stream = stub->Session(&context);

        NStreamingTest::TSessionResponse response;

        UNIT_ASSERT_VALUES_EQUAL(stream->Read(&response), true);
        UNIT_ASSERT_VALUES_EQUAL(response.GetResponseCookie(), 123u);

        UNIT_ASSERT_VALUES_EQUAL(stream->Read(&response), true);
        UNIT_ASSERT_VALUES_EQUAL(response.GetResponseCookie(), 456u);

        auto status = stream->Finish();
        UNIT_ASSERT(status.ok());
    }

    // Regression test for LOGBROKER-10618.
    // A streaming RPC is left hanging (the server actor holds the IStreamCtx
    // and never finishes). When the gRPC server is stopped, its completion
    // queues are shut down. Then the actor system is destroyed, which destroys
    // the hanging actor and its IStreamCtx (TFacade). The facade destructor
    // calls Detach() -> FinishInternal() -> Stream.Finish() on the already
    // dead completion queue, which used to hit the Y_VERIFY
    // "grpc_cq_begin_op(cq_, notify_tag) failed".
    Y_UNIT_TEST(FinishAfterServerShutdownDoesNotCrash) {
        NYdbGrpc::TServerOptions options;
        // Short deadline so Stop() does not block for the default 30s while
        // the hanging RPC keeps the server from draining.
        options.SetGRpcShutdownDeadline(TDuration::MilliSeconds(100));

        // Per-test event: a fresh event for each test run, so a failure in a
        // previous run cannot make WaitI() return early on a stale signal.
        TManualEvent attached;
        THangActor::Attached = &attached;

        auto server = MakeHolder<TGRpcTestServer<THangActor>>(std::move(options));

        auto channel = grpc::CreateChannel(server->GRpcEndpoint, grpc::InsecureChannelCredentials());
        auto stub = NStreamingTest::TStreamingService::NewStub(channel);

        // The server actor (THangActor) now holds the IStreamCtx and never
        // finishes the stream. Keep the client stream alive across server.Reset()
        // so the RPC stays in flight on the server side (with no in-flight write:
        // THangActor never writes, so FlagWriteActive stays clear) when the
        // completion queues are shut down, matching LOGBROKER-10618.
        grpc::ClientContext context;
        auto stream = stub->Session(&context);
        Y_UNUSED(stream);

        // Wait until the server-side THangActor has been created and attached
        // to the stream. This guarantees the IStreamCtx (TFacade) is held by
        // the actor before we shut the server down, so the facade destructor
        // will run after the completion queue is dead.
        attached.WaitI();

        // Destroy the server: GRpcServer->Stop() shuts down the CQs, then the
        // actor system is destroyed, destroying THangActor and its IStreamCtx.
        // Without the fix this crashes with the LOGBROKER-10618 Y_VERIFY.
        server.Reset();

        THangActor::Attached = nullptr;
    }
}

}
}
}
