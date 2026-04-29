#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>
#include <ydb/core/grpc_services/grpc_request_check_actor.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

namespace {

const TString GrpcRequestProxySpanName = "GrpcRequestProxy";

class TTestFacilityProvider : public NGRpcService::IFacilityProvider {
public:
    ui64 GetChannelBufferSize() const override {
        return 0;
    }

    TActorId RegisterActor(IActor* actor) const override {
        return TActivationContext::AsActorContext().Register(actor);
    }
};

struct TTestSetup {
    const TString UserSid;
    const TString DbPath;
    TTestActorRuntime Runtime;
    TTestFacilityProvider FacilityProvider;
    TActorId FakeMonActor;
    TSchemeBoardEvents::TDescribeSchemeResult DescribeSchemeResult;

    TTestSetup(const TString& userSid, const TString& dbPath)
        : UserSid(userSid)
        , DbPath(dbPath)
    {
        Runtime.Initialize(TAppPrepare().Unwrap());
        StartYdb();
        DescribeSchemeResult.SetPath(DbPath);
    }

    TTestActorRuntime* GetRuntime() {
        return &Runtime;
    }

    void StartYdb() {
        Runtime.GetAppData().EnforceUserTokenRequirement = true;
        Runtime.GetAppData().DefaultUserSIDs = {UserSid};
        Runtime.GetAppData().Counters = new NMonitoring::TDynamicCounters;
        FakeMonActor = Runtime.AllocateEdgeActor();
    }

    template <typename TEvType>
    void RequestCheckActor(std::unique_ptr<TEvType>&& ev) {
        std::unique_ptr<IEventHandle> ieh = std::make_unique<IEventHandle>(NGRpcService::CreateGRpcRequestProxyId(),
            FakeMonActor,
            ev.release(),
            IEventHandle::FlagTrackDelivery);

        TAutoPtr<TEventHandle<TEvType>> request = reinterpret_cast<TEventHandle<TEvType>*>(ieh.release());

        TActorId fakeGrpcRequestProxy = Runtime.AllocateEdgeActor();
        Runtime.Register(CreateGrpcRequestCheckActor<TEvType>(fakeGrpcRequestProxy,
            DescribeSchemeResult,
            TIntrusivePtr<TSecurityObject>{},
            request,
            NGRpcService::CreateGRpcProxyCounters(Runtime.GetAppData().Counters),
            true,
            {},
            &FacilityProvider,
            NGRpcService::TCloudPermissionsSettings{}));
    }
};

struct TEvRuntimeRequestPassed : public TEventLocal<TEvRuntimeRequestPassed, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    explicit TEvRuntimeRequestPassed(ui32 finishSpanCalls)
        : FinishSpanCalls(finishSpanCalls)
    {}

    ui32 FinishSpanCalls;
};

class TTestProxyRuntimeEvent : public NGRpcService::TEvProxyRuntimeEvent {
public:
    TTestProxyRuntimeEvent(TString database, TMaybe<TString> token, TActorId replyTo)
        : Database_(std::move(database))
        , Token_(std::move(token))
        , ReplyTo_(replyTo)
        , AuthState_(true)
    {}

    const TMaybe<TString> GetYdbToken() const override {
        return Token_;
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        AuthState_.State = state;
    }

    void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        InternalToken_ = token;
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        return AuthState_;
    }

    void ReplyUnauthenticated(const TString& = {}) override {
        ReplyWithYdbStatus(Ydb::StatusIds::UNAUTHORIZED);
    }

    void RaiseIssue(const NYql::TIssue&) override {
    }

    void RaiseIssues(const NYql::TIssues&) override {
    }

    TVector<TStringBuf> FindClientCertPropertyValues() const override {
        return {};
    }

    void StartTracing(NWilson::TSpan&&) override {
    }

    void FinishSpan() override {
        ++FinishSpanCalls_;
    }

    bool* IsTracingDecided() override {
        return nullptr;
    }

    bool Validate(TString&) override {
        return true;
    }

    void SetCounters(NGRpcService::IGRpcProxyCounters::TPtr counters) override {
        Counters_ = std::move(counters);
    }

    NGRpcService::IGRpcProxyCounters::TPtr GetCounters() const override {
        return Counters_;
    }

    void UseDatabase(const TString& database) override {
        Database_ = database;
    }

    void SetRespHook(NGRpcService::TRespHook&&) override {
    }

    void SetRlPath(TMaybe<NRpcService::TRlPath>&& path) override {
        RlPath_ = std::move(path);
    }

    NGRpcService::TRateLimiterMode GetRlMode() const override {
        return NGRpcService::TRateLimiterMode::Off;
    }

    bool TryCustomAttributeProcess(const NKikimrScheme::TEvDescribeSchemeResult&, NGRpcService::ICheckerIface*) override {
        return false;
    }

    void Pass(const NGRpcService::IFacilityProvider&) override {
        TActivationContext::AsActorContext().Send(ReplyTo_, new TEvRuntimeRequestPassed(FinishSpanCalls_));
        delete this;
    }

    void SetAuditLogHook(NGRpcService::TAuditLogHook&&) override {
    }

    void SetDiskQuotaExceeded(bool) override {
    }

    TString GetRpcMethodName() const override {
        return "TestRuntimeRequest";
    }

    TMaybe<TString> GetTraceId() const override {
        return {};
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return {};
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return Database_;
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return InternalToken_;
    }

    const TString& GetSerializedToken() const override {
        if (InternalToken_) {
            return InternalToken_->GetSerializedToken();
        }
        return EmptySerializedToken_;
    }

    bool IsClientLost() const override {
        return false;
    }

    const TMaybe<TString> GetPeerMetaValues(const TString&) const override {
        return {};
    }

    TString GetPeerName() const override {
        return "127.0.0.1";
    }

    const TString& GetRequestName() const override {
        static const TString name = "TestRuntimeRequest";
        return name;
    }

    TMaybe<NRpcService::TRlPath> GetRlPath() const override {
        return RlPath_;
    }

    TInstant GetDeadline() const override {
        return TInstant::Max();
    }

    void AddAuditLogPart(const TStringBuf&, const TString&) override {
    }

    const NGRpcService::TAuditLogParts& GetAuditLogParts() const override {
        static const NGRpcService::TAuditLogParts emptyAuditLogParts;
        return emptyAuditLogParts;
    }

private:
    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode) override {
        TActivationContext::AsActorContext().Send(ReplyTo_, new TEvRuntimeRequestPassed(FinishSpanCalls_));
    }

private:
    TString Database_;
    TMaybe<TString> Token_;
    TActorId ReplyTo_;
    NYdbGrpc::TAuthState AuthState_;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken_;
    NGRpcService::IGRpcProxyCounters::TPtr Counters_;
    TMaybe<NRpcService::TRlPath> RlPath_;
    ui32 FinishSpanCalls_ = 0;
    inline static const TString EmptySerializedToken_;
};

class TTestGrpcRequestContext : public NYdbGrpc::IRequestContextBase {
public:
    const NProtoBuf::Message* GetRequest() const override {
        return &Request_;
    }

    NYdbGrpc::TAuthState& GetAuthState() override {
        return AuthState_;
    }

    void Reply(NProtoBuf::Message*, ui32 = 0) override {
        ++ReplyCount;
    }

    void Reply(grpc::ByteBuffer*, ui32 = 0, NYdbGrpc::IRequestContextBase::EStreamCtrl = NYdbGrpc::IRequestContextBase::EStreamCtrl::CONT) override {
        ++ByteReplyCount;
    }

    void ReplyUnauthenticated(const TString&) override {
        ++ReplyUnauthenticatedCount;
    }

    void ReplyError(grpc::StatusCode, const TString&, const TString& = "") override {
        ++ReplyErrorCount;
    }

    TInstant Deadline() const override {
        return TInstant::Max();
    }

    TSet<TStringBuf> GetPeerMetaKeys() const override {
        return {};
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf) const override {
        return {};
    }

    TVector<TStringBuf> FindClientCert() const override {
        return {};
    }

    grpc_compression_level GetCompressionLevel() const override {
        return GRPC_COMPRESS_LEVEL_NONE;
    }

    google::protobuf::Arena* GetArena() override {
        return &Arena_;
    }

    void AddTrailingMetadata(const TString&, const TString&) override {
    }

    void UseDatabase(const TString&) override {
    }

    void SetNextReplyCallback(NYdbGrpc::IRequestContextBase::TOnNextReply&&) override {
    }

    bool IsStreamCall() const override {
        return false;
    }

    void FinishStreamingOk() override {
        ++FinishStreamingOkCount;
    }

    NYdbGrpc::IRequestContextBase::TAsyncFinishResult GetFinishFuture() override {
        return NThreading::MakeFuture(NYdbGrpc::IRequestContextBase::EFinishStatus::OK);
    }

    TString GetPeer() const override {
        return "127.0.0.1";
    }

    bool SslServer() const override {
        return false;
    }

    bool IsClientLost() const override {
        return false;
    }

    TString GetEndpointId() const override {
        return "test-endpoint";
    }

    TString GetRpcMethodName() const override {
        return "Ydb.Operations.CancelOperation";
    }

public:
    ui32 ReplyCount = 0;
    ui32 ByteReplyCount = 0;
    ui32 ReplyUnauthenticatedCount = 0;
    ui32 ReplyErrorCount = 0;
    ui32 FinishStreamingOkCount = 0;

private:
    Ydb::Operations::CancelOperationRequest Request_;
    NYdbGrpc::TAuthState AuthState_{true};
    google::protobuf::Arena Arena_;
};

class TTestBiStreamContext
    : public NGRpcServer::IGRpcStreamingContext<Draft::Dummy::PingRequest, Draft::Dummy::PingResponse>
{
public:
    void Cancel() override {
    }

    void Attach(TActorId) override {
        ++AttachCount;
    }

    bool Read() override {
        return true;
    }

    bool Finish(const grpc::Status&) override {
        ++FinishCount;
        return true;
    }

    NYdbGrpc::TAuthState& GetAuthState() const override {
        return AuthState_;
    }

    TString GetPeerName() const override {
        return "127.0.0.1";
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf) const override {
        return {};
    }

    grpc_compression_level GetCompressionLevel() const override {
        return GRPC_COMPRESS_LEVEL_NONE;
    }

    void UseDatabase(const TString&) override {
    }

    TString GetRpcMethodName() const override {
        return "Draft.Dummy.Ping";
    }

    bool Write(Draft::Dummy::PingResponse&&, const grpc::WriteOptions& = {}) override {
        return true;
    }

    bool WriteAndFinish(Draft::Dummy::PingResponse&&, const grpc::Status&) override {
        ++WriteAndFinishCount;
        return true;
    }

    bool WriteAndFinish(Draft::Dummy::PingResponse&&, const grpc::WriteOptions&, const grpc::Status&) override {
        ++WriteAndFinishWithOptionsCount;
        return true;
    }

public:
    ui32 AttachCount = 0;
    ui32 FinishCount = 0;
    ui32 WriteAndFinishCount = 0;
    ui32 WriteAndFinishWithOptionsCount = 0;

private:
    mutable NYdbGrpc::TAuthState AuthState_{true};
};

using TTestGrpcRequest = NGRpcService::TGrpcRequestNoOperationCall<
    Ydb::Operations::CancelOperationRequest,
    Ydb::Operations::CancelOperationResponse>;

class TInvalidBiStreamPingRequest : public NGRpcService::TEvBiStreamPingRequest {
public:
    using NGRpcService::TEvBiStreamPingRequest::TEvBiStreamPingRequest;

    bool Validate(TString& error) override {
        error = "invalid request";
        return false;
    }
};

class TPublicProxyHandleMethods : public NGRpcService::TGRpcRequestProxyHandleMethods {
public:
    using NGRpcService::TGRpcRequestProxyHandleMethods::ValidateAndReplyOnError;
};

NWilson::TFakeWilsonUploader* SetupFakeWilsonUploader(TTestActorRuntime& runtime) {
    auto* uploader = new NWilson::TFakeWilsonUploader;
    auto actorId = runtime.Register(uploader);
    runtime.RegisterService(NWilson::MakeWilsonUploaderId(), actorId);
    return uploader;
}

NWilson::TSpan MakeGrpcRequestProxySpan(TTestActorRuntime& runtime) {
    return NWilson::TSpan(
        TWilsonGrpc::RequestProxy,
        NWilson::TTraceId::NewTraceId(15, 4095),
        GrpcRequestProxySpanName,
        NWilson::EFlags::NONE,
        runtime.GetActorSystem(0));
}

ui32 CountSpansByName(const NWilson::TFakeWilsonUploader& uploader, const TString& name) {
    ui32 count = 0;
    for (const auto& span : uploader.Spans) {
        if (span.name() == name) {
            ++count;
        }
    }
    return count;
}

void DispatchUntilSpanCount(TTestActorRuntime& runtime, const NWilson::TFakeWilsonUploader& uploader, const TString& name, ui32 expectedCount) {
    TDispatchOptions options;
    options.CustomFinalCondition = [&] {
        return CountSpansByName(uploader, name) >= expectedCount;
    };
    runtime.DispatchEvents(options, TDuration::MilliSeconds(1));
}

void AssertGrpcRequestProxySpanNotSent(const NWilson::TFakeWilsonUploader& uploader) {
    UNIT_ASSERT_VALUES_EQUAL(CountSpansByName(uploader, GrpcRequestProxySpanName), 0);
}

void AssertGrpcRequestProxySpanSentOnce(const NWilson::TFakeWilsonUploader& uploader) {
    UNIT_ASSERT_VALUES_EQUAL(CountSpansByName(uploader, GrpcRequestProxySpanName), 1);
}

class TWilsonEventCounter {
public:
    explicit TWilsonEventCounter(TTestActorRuntime& runtime)
        : Runtime_(runtime)
    {
        PreviousFilter_ = Runtime_.SetEventFilter([this](NActors::TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == NWilson::TEvWilson::EventType) {
                ++Count_;
            }
            return PreviousFilter_(runtime, event);
        });
    }

    ~TWilsonEventCounter() {
        Runtime_.SetEventFilter(std::move(PreviousFilter_));
    }

    ui32 GetCount() const {
        return Count_;
    }

private:
    TTestActorRuntime& Runtime_;
    NActors::TTestActorRuntimeBase::TEventFilter PreviousFilter_;
    ui32 Count_ = 0;
};

template <typename TAction>
void AssertGrpcRequestProxySpanSentAfter(TTestActorRuntime& runtime, const NWilson::TFakeWilsonUploader& uploader, TAction&& action) {
    AssertGrpcRequestProxySpanNotSent(uploader);

    TWilsonEventCounter wilsonEvents(runtime);
    action();
    UNIT_ASSERT_VALUES_EQUAL(wilsonEvents.GetCount(), 1);
    DispatchUntilSpanCount(runtime, uploader, GrpcRequestProxySpanName, 1);

    AssertGrpcRequestProxySpanSentOnce(uploader);
}

} // namespace

Y_UNIT_TEST_SUITE(TGrpcRequestCheckActorTracing) {

Y_UNIT_TEST(DoesNotFinishGrpcRequestProxySpanBeforeRuntimeEventPass) {
    TTestSetup setup("user1", "/Root/db");

    std::unique_ptr<NGRpcService::TEvProxyRuntimeEvent> ev = std::make_unique<TTestProxyRuntimeEvent>(
        setup.DbPath,
        Nothing(),
        setup.FakeMonActor);

    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    auto* result = setup.GetRuntime()->GrabEdgeEvent<TEvRuntimeRequestPassed>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->FinishSpanCalls, 0);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanForAuthAndCheckRequest) {
    TTestSetup setup("user1", "/Root/db");
    auto* uploader = SetupFakeWilsonUploader(*setup.GetRuntime());

    std::unique_ptr<NGRpcService::TEvRequestAuthAndCheck> ev = std::make_unique<NGRpcService::TEvRequestAuthAndCheck>(
        setup.DbPath,
        Nothing(),
        setup.FakeMonActor,
        NGRpcService::TAuditMode::NonModifying(),
        "192.168.0.101");
    ev->StartTracing(MakeGrpcRequestProxySpan(*setup.GetRuntime()));
    AssertGrpcRequestProxySpanNotSent(*uploader);

    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    auto* result = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TEvRequestAuthAndCheckResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    DispatchUntilSpanCount(*setup.GetRuntime(), *uploader, GrpcRequestProxySpanName, 1);

    AssertGrpcRequestProxySpanSentOnce(*uploader);
}

} // TGrpcRequestCheckActorTracing

Y_UNIT_TEST_SUITE(TGrpcRequestBaseTracing) {

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnFinalReply) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 1);
}

Y_UNIT_TEST(SecondFinishSpanDoesNotEmitAnotherWilsonSpan) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
    });
    {
        TWilsonEventCounter wilsonEvents(runtime);
        request.FinishSpan();
        UNIT_ASSERT_VALUES_EQUAL(wilsonEvents.GetCount(), 0);
    }

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 1);
    AssertGrpcRequestProxySpanSentOnce(*uploader);
}

Y_UNIT_TEST(ManualFinishSpanEmitsGrpcRequestProxySpan) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.FinishSpan();
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 0);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnRpcStatusError) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.ReplyWithRpcStatus(grpc::StatusCode::UNAVAILABLE, "unavailable", "details");
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyErrorCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnUnauthenticatedReply) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.ReplyUnauthenticated("unauthenticated");
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyUnauthenticatedCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnGrpcError) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.ReplyGrpcError(grpc::StatusCode::INTERNAL, "internal", "details");
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyErrorCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnUnarySerializedResult) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.SendSerializedResult(TString("serialized-result"), Ydb::StatusIds::SUCCESS);
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->ByteReplyCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnUnarySerializedRopeResult) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.SendSerializedResult(TRope(TString("serialized-rope-result")), Ydb::StatusIds::SUCCESS);
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->ByteReplyCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnFinishStream) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.FinishStream(Ydb::StatusIds::SUCCESS);
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->FinishStreamingOkCount, 1);
}

Y_UNIT_TEST(RespHookDelaysGrpcRequestProxySpanUntilPass) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    NGRpcService::TRespHookCtx::TPtr delayedReply;
    request.SetRespHook([&](NGRpcService::TRespHookCtx::TPtr hookCtx) {
        delayedReply = std::move(hookCtx);
    });

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanNotSent(*uploader);
    TWilsonEventCounter wilsonEvents(runtime);
    request.ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);

    UNIT_ASSERT(delayedReply);
    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(wilsonEvents.GetCount(), 0);
    AssertGrpcRequestProxySpanNotSent(*uploader);

    delayedReply->Pass();
    UNIT_ASSERT_VALUES_EQUAL(wilsonEvents.GetCount(), 1);
    DispatchUntilSpanCount(runtime, *uploader, GrpcRequestProxySpanName, 1);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 1);
    AssertGrpcRequestProxySpanSentOnce(*uploader);
}

} // TGrpcRequestBaseTracing

Y_UNIT_TEST_SUITE(TGrpcRequestBiStreamTracing) {

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnValidationErrorReply) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    TInvalidBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    bool valid = true;
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        valid = TPublicProxyHandleMethods::ValidateAndReplyOnError(&request);
    });

    UNIT_ASSERT(!valid);
    UNIT_ASSERT_VALUES_EQUAL(ctx->AttachCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(ctx->WriteAndFinishCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnUnauthenticatedReply) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    NGRpcService::TEvBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.ReplyUnauthenticated("unauthenticated");
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->FinishCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnFinish) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    NGRpcService::TEvBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.Finish(Ydb::StatusIds::SUCCESS);
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->FinishCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnWriteAndFinish) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    NGRpcService::TEvBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.WriteAndFinish(Draft::Dummy::PingResponse(), Ydb::StatusIds::SUCCESS);
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->WriteAndFinishCount, 1);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnWriteAndFinishWithOptions) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    NGRpcService::TEvBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    AssertGrpcRequestProxySpanSentAfter(runtime, *uploader, [&] {
        request.WriteAndFinish(Draft::Dummy::PingResponse(), Ydb::StatusIds::SUCCESS, grpc::WriteOptions());
    });

    UNIT_ASSERT_VALUES_EQUAL(ctx->WriteAndFinishWithOptionsCount, 1);
}

} // TGrpcRequestBiStreamTracing
