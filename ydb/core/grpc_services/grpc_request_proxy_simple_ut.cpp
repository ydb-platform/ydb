#include "grpc_request_proxy.h"

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {
namespace {

template <typename TRequest>
class TTestRequestContext : public NYdbGrpc::IRequestContextBase {
public:
    explicit TTestRequestContext(TString database)
        : Database(std::move(database))
    {}

    const NProtoBuf::Message* GetRequest() const override {
        return &Request;
    }

    NYdbGrpc::TAuthState& GetAuthState() override {
        return AuthState;
    }

    void Reply(NProtoBuf::Message*, ui32 status = 0) override {
        Finish(status);
    }

    void Reply(grpc::ByteBuffer*, ui32 status = 0,
        NYdbGrpc::IRequestContextBase::EStreamCtrl = NYdbGrpc::IRequestContextBase::EStreamCtrl::CONT) override
    {
        Finish(status);
    }

    void ReplyUnauthenticated(const TString&) override {
        Finish(Ydb::StatusIds::UNAUTHORIZED);
    }

    void ReplyError(grpc::StatusCode, const TString&, const TString& = "") override {
        Finish(Ydb::StatusIds::GENERIC_ERROR);
    }

    TInstant Deadline() const override {
        return TInstant::Max();
    }

    TSet<TStringBuf> GetPeerMetaKeys() const override {
        return {};
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
        if (key == NYdb::YDB_DATABASE_HEADER && Database) {
            return {Database};
        }
        return {};
    }

    TVector<TStringBuf> FindClientCert() const override {
        return {};
    }

    grpc_compression_level GetCompressionLevel() const override {
        return GRPC_COMPRESS_LEVEL_NONE;
    }

    google::protobuf::Arena* GetArena() override {
        return &Arena;
    }

    void AddTrailingMetadata(const TString&, const TString&) override {}
    void UseDatabase(const TString&) override {}
    void SetNextReplyCallback(NYdbGrpc::IRequestContextBase::TOnNextReply&&) override {}

    bool IsStreamCall() const override {
        return false;
    }

    void FinishStreamingOk() override {
        Finish(Ydb::StatusIds::SUCCESS);
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
        return TRequest::descriptor()->full_name();
    }

    TRequest Request;
    const TString Database;
    TString EffectiveDatabase;
    NYdbGrpc::TAuthState AuthState{false};
    TMaybe<ui32> Status;
    std::function<void()> OnReply;

private:
    void Finish(ui32 status) {
        if (OnReply) {
            OnReply();
        }
        Status = status;
    }

    google::protobuf::Arena Arena;
};

using TRuntimeRequest = TGrpcRequestCall<Ydb::Operations::CancelOperationRequest,
    Ydb::Operations::CancelOperationResponse, false>;
using TRuntimeContext = TTestRequestContext<Ydb::Operations::CancelOperationRequest>;
using TDiscoveryContext = TTestRequestContext<Ydb::Discovery::ListEndpointsRequest>;

class TInternalRuntimeRequest : public TRuntimeRequest {
public:
    using TRuntimeRequest::TRuntimeRequest;

    bool IsInternalCall() const override {
        return true;
    }
};

struct TSimpleProxySetup {
    TTestActorRuntime Runtime;
    TActorId Proxy;

    explicit TSimpleProxySetup(bool ignoreRoot, const TString& root = "backup") {
        TAppPrepare app;
        if (root) {
            app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain(root).Release());
        }
        Runtime.Initialize(app.Unwrap());
        Runtime.GetAppData().Counters = new NMonitoring::TDynamicCounters;
        NKikimrConfig::TAppConfig config;
        config.MutableGRpcConfig()->SetIgnoreRoot(ignoreRoot);
        config.MutableGRpcConfig()->SetHost("localhost");
        config.MutableGRpcConfig()->SetPort(2135);
        Proxy = Runtime.Register(CreateGRpcRequestProxySimple(config));
    }

    template <typename TEvent, typename TContext>
    void Send(TEvent* request, const TIntrusivePtr<TContext>& context) {
        context->OnReply = [request, context = context.Get()] {
            context->EffectiveDatabase = request->GetDatabaseName().GetOrElse("");
        };
        Runtime.SendAsync(new IEventHandle(Proxy, {}, request));
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return context->Status.Defined(); };
        Runtime.DispatchEvents(options);
        UNIT_ASSERT(context->Status);
    }

    TIntrusivePtr<TRuntimeContext> RuntimeRequest(const TString& database, bool internal = false, bool authFailure = false) {
        auto context = MakeIntrusive<TRuntimeContext>(database);
        context->Request.set_id("operation-id");
        if (authFailure) {
            context->AuthState.NeedAuth = true;
            context->AuthState.State = NYdbGrpc::TAuthState::AS_FAIL;
        }
        auto callback = [](std::unique_ptr<IRequestNoOpCtx> request, const IFacilityProvider&) {
            request->ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
        };
        TRuntimeRequest* request = internal
            ? new TInternalRuntimeRequest(context.Get(), callback)
            : new TRuntimeRequest(context.Get(), callback);
        Send(request, context);
        return context;
    }

    TIntrusivePtr<TDiscoveryContext> DiscoveryRequest(const TString& body, const TString& header, bool authFailure = false) {
        auto context = MakeIntrusive<TDiscoveryContext>(header);
        context->Request.set_database(body);
        if (authFailure) {
            context->AuthState.NeedAuth = true;
            context->AuthState.State = NYdbGrpc::TAuthState::AS_FAIL;
        }
        Send(new TEvListEndpointsRequest(context.Get()), context);
        return context;
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TGrpcRequestProxySimpleIgnoreRoot) {
    Y_UNIT_TEST(ResolvesRuntimeRequestsBeforeDispatch) {
        TSimpleProxySetup setup(true);
        for (const auto& [database, expected] : TVector<std::pair<TString, TString>>{
            {"/ru", "/backup"},
            {"/ru/", "/backup"},
            {"/ru/team/db", "/backup/team/db"},
            {"/kfront", "/backup"},
            {"/backup", "/backup"},
            {"/backup/team/db", "/backup/team/db"},
            {"ru", "ru"},
            {"team/db", "team/db"},
            {"", ""},
        }) {
            auto context = setup.RuntimeRequest(database);
            UNIT_ASSERT(*context->Status == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(context->EffectiveDatabase, expected);
        }
    }

    Y_UNIT_TEST(ResolvesBeforeAuthenticationFailure) {
        TSimpleProxySetup setup(true);
        auto context = setup.RuntimeRequest("/ru/db", false, true);
        UNIT_ASSERT(*context->Status == Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(context->EffectiveDatabase, "/backup/db");
    }

    Y_UNIT_TEST(SkipsInternalRequests) {
        TSimpleProxySetup setup(true);
        for (const auto& database : TVector<TString>{"/ru", "/ru/db"}) {
            auto context = setup.RuntimeRequest(database, true);
            UNIT_ASSERT(*context->Status == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(context->EffectiveDatabase, database);
        }
    }

    Y_UNIT_TEST(DisabledDoesNotRequireDomain) {
        TSimpleProxySetup setup(false, "");
        for (const auto& database : TVector<TString>{"/ru", "/ru/db", "/backup", "ru", "team/db"}) {
            auto context = setup.RuntimeRequest(database);
            UNIT_ASSERT(*context->Status == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(context->EffectiveDatabase, database);
        }
        auto discovery = setup.DiscoveryRequest("/ru/db", "/ru/other");
        UNIT_ASSERT(*discovery->Status == Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(discovery->EffectiveDatabase, "/ru/other");
    }

    Y_UNIT_TEST(ResolvesDiscoveryBodyAndHeader) {
        TSimpleProxySetup setup(true);
        for (const auto& body : TVector<TString>{"/ru", "/ru/", "/backup"}) {
            for (const auto& header : TVector<TString>{"", "/ru", "/backup"}) {
                auto context = setup.DiscoveryRequest(body, header);
                UNIT_ASSERT(*context->Status == Ydb::StatusIds::SUCCESS);
                UNIT_ASSERT_VALUES_EQUAL(context->EffectiveDatabase, "/backup");
            }
        }
        for (const auto& header : TVector<TString>{"", "/ru/team/db", "/backup/team/db"}) {
            auto context = setup.DiscoveryRequest("/ru/team/db", header);
            UNIT_ASSERT(*context->Status == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(context->EffectiveDatabase, "/backup/team/db");
        }
    }

    Y_UNIT_TEST(RejectsDiscoveryMismatchBeforeAuthentication) {
        TSimpleProxySetup setup(true);
        for (const auto& [body, header] : TVector<std::pair<TString, TString>>{
            {"/ru/db", "/ru/other"},
            {"/ru", "/backup/ru"},
            {"/ru/db", "/backup"},
        }) {
            auto context = setup.DiscoveryRequest(body, header, true);
            UNIT_ASSERT(*context->Status == Ydb::StatusIds::BAD_REQUEST);
        }
    }
}

} // namespace NKikimr::NGRpcService
