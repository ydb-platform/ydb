#include <ydb/core/testlib/test_client.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_request_check_actor.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/cloud_permissions/cloud_permissions.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NKikimr;
using namespace Tests;

namespace {

struct TTestSetup {
    TPortManager PortManager;
    const ui16 KikimrPort;
    const TString AccessServiceEndpoint;
    TTicketParserAccessServiceMockV2 AccessServiceMock;
    std::unique_ptr<grpc::Server> AccessServiceServer;
    std::unique_ptr<TServer> Server;
    TActorId FakeMonActor;
    TSchemeBoardEvents::TDescribeSchemeResult DescribeSchemeResult;
    TVector<std::pair<TString, TString>> RootAttributes;
    const TString UserSid;
    const TString DbPath;

    static const TString CloudId;
    static const TString FolderId;
    static const TString DatabaseId;
    static const TString ClusterCloudId;
    static const TString ClusterFolderId;
    static const TString GizmoId;

    static const THashSet<TString> GizmoPermissions;
    static const THashSet<TString> ClusterPermissions;

    TTestSetup(const TString& userSid, const TString& dbPath, const std::vector<std::pair<TString, TString>>& userAttributes)
        : KikimrPort(PortManager.GetPort(2134))
        , AccessServiceEndpoint("localhost:" + ToString(PortManager.GetPort(4284)))
        , Server(std::make_unique<TServer>(GetSettings()))
        , UserSid(userSid)
        , DbPath(dbPath)
    {
        StartAccessServiceMock();
        StartYdb();
        InitializeDb(userAttributes);
    }

    TTestActorRuntime* GetRuntime() {
        return Server->GetRuntime();
    }

    TServerSettings GetSettings() {
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetAccessServiceType("Yandex_v2");
        authConfig.SetUseAccessServiceApiKey(false);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(AccessServiceEndpoint);
        authConfig.SetUseStaff(false);

        auto settings = TServerSettings(KikimrPort, authConfig);
        settings.SetEnableAccessServiceBulkAuthorization(true);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        return settings;
    }

    void StartAccessServiceMock() {
        InitAccessServiceMock();
        grpc::ServerBuilder accessServiceServerBuilder;
        accessServiceServerBuilder.AddListeningPort(AccessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&AccessServiceMock);
        AccessServiceServer = accessServiceServerBuilder.BuildAndStart();
    }

    void InitAccessServiceMock() {
        AccessServiceMock.AllowedResourceIds = {
            CloudId,
            FolderId,
            DatabaseId,
            ClusterCloudId,
            ClusterFolderId,
            GizmoId
        };

        AccessServiceMock.AllowedUserPermissions.clear();
        for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
            AccessServiceMock.AllowedUserPermissions.insert(UserSid + "-" + permission);
        }
        for (const auto& permission : GizmoPermissions) {
            AccessServiceMock.AllowedUserPermissions.insert(UserSid + "-" + permission);
        }
        for (const auto& permission : ClusterPermissions) {
            AccessServiceMock.AllowedUserPermissions.insert(UserSid + "-" + permission);
        }
    }

    void StartYdb() {
        TTestActorRuntime* runtime = Server->GetRuntime();
        runtime->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        runtime->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        runtime->SetLogPriority(NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_DEBUG);
        runtime->GetAppData().EnforceUserTokenRequirement = true;
        FakeMonActor = runtime->AllocateEdgeActor();
    }

    void InitializeDb(const std::vector<std::pair<TString, TString>>& userAttributes) {
        DescribeSchemeResult.SetPath(DbPath);
        auto pathDescription = DescribeSchemeResult.MutablePathDescription();
        for (const auto& [key, value] : userAttributes) {
            auto userAttribute = pathDescription->AddUserAttributes();
            userAttribute->SetKey(key);
            userAttribute->SetValue(value);
        }

        RootAttributes =  {
            {"cloud_id", ClusterCloudId},
            {"folder_id", ClusterFolderId}
        };
    }

    template <typename TEvType>
    void RequestCheckActor(std::unique_ptr<TEvType>&& ev) {
        NACLib::TSecurityObject object("owner", false);
        object.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::ConnectDatabase, UserSid);
        TIntrusivePtr<TSecurityObject> securityObject = MakeIntrusive<TSecurityObject>(object.GetOwnerSID(), object.GetACL().SerializeAsString(), false);

        std::unique_ptr<IEventHandle> ieh = std::make_unique<IEventHandle>(NGRpcService::CreateGRpcRequestProxyId(),
            FakeMonActor,
            ev.release(),
            IEventHandle::FlagTrackDelivery);

        TAutoPtr<TEventHandle<TEvType>> request = reinterpret_cast<TEventHandle<TEvType>*>(ieh.release());

        TTestActorRuntime* runtime = Server->GetRuntime();
        TActorId fakeGrpcRequestProxy = runtime->AllocateEdgeActor();
        runtime->Register(CreateGrpcRequestCheckActor<TEvType>(fakeGrpcRequestProxy,
            DescribeSchemeResult,
            securityObject,
            request,
            NGRpcService::CreateGRpcProxyCounters(runtime->GetAppData().Counters), // Counters
            false,
            RootAttributes,
            nullptr, // FacilityProvider
            {
                .UseAccessService = true,
                .NeedClusterAccessResourceCheck = true,
                .AccessServiceType = runtime->GetAppData().AuthConfig.GetAccessServiceType()
            }));
    }
};

const TString TTestSetup::CloudId = "cloud12345";
const TString TTestSetup::FolderId = "folder12345";
const TString TTestSetup::DatabaseId = "database12345";
const TString TTestSetup::ClusterCloudId = "cluster.cloud98765";
const TString TTestSetup::ClusterFolderId = "cluster.folder98765";
const TString TTestSetup::GizmoId = "gizmo";

const THashSet<TString> TTestSetup::GizmoPermissions {
    "ydb.developerApi.get",
    "ydb.developerApi.update",
};

const THashSet<TString> TTestSetup::ClusterPermissions {
    "ydb.clusters.get",
    "ydb.clusters.monitor",
    "ydb.clusters.manage",
};

struct TEvRuntimeRequestPassed : public TEventLocal<TEvRuntimeRequestPassed, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    explicit TEvRuntimeRequestPassed(ui32 finishSpanCalls)
        : FinishSpanCalls(finishSpanCalls)
    {}

    ui32 FinishSpanCalls;
};

class TTestProxyRuntimeEvent : public NGRpcService::TEvProxyRuntimeEvent {
public:
    TTestProxyRuntimeEvent(TString database, TString token, TActorId replyTo)
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

    void RaiseIssue(const NYql::TIssue& issue) override {
        Issues_.AddIssue(issue);
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        Issues_.AddIssues(issues);
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

    void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
        AuditLogParts_.emplace_back(name, value);
    }

    const NGRpcService::TAuditLogParts& GetAuditLogParts() const override {
        return AuditLogParts_;
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
    NYql::TIssues Issues_;
    NGRpcService::IGRpcProxyCounters::TPtr Counters_;
    TMaybe<NRpcService::TRlPath> RlPath_;
    NGRpcService::TAuditLogParts AuditLogParts_;
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

    void Reply(NProtoBuf::Message*, ui32 status = 0) override {
        ++ReplyCount;
        LastStatus = status;
    }

    void Reply(grpc::ByteBuffer*, ui32 status = 0, NYdbGrpc::IRequestContextBase::EStreamCtrl ctrl = NYdbGrpc::IRequestContextBase::EStreamCtrl::CONT) override {
        ++ByteReplyCount;
        LastStatus = status;
        LastStreamCtrl = ctrl;
    }

    void ReplyUnauthenticated(const TString& in) override {
        ++ReplyUnauthenticatedCount;
        LastErrorMessage = in;
    }

    void ReplyError(grpc::StatusCode code, const TString& msg, const TString& details = "") override {
        ++ReplyErrorCount;
        LastGrpcStatus = code;
        LastErrorMessage = msg;
        LastErrorDetails = details;
    }

    TInstant Deadline() const override {
        return TInstant::Max();
    }

    TSet<TStringBuf> GetPeerMetaKeys() const override {
        return {};
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
        const auto it = PeerMeta_.find(TString(key));
        if (it == PeerMeta_.end()) {
            return {};
        }
        return {it->second};
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

    void AddTrailingMetadata(const TString& key, const TString& value) override {
        TrailingMetadata_.emplace_back(key, value);
    }

    void UseDatabase(const TString& database) override {
        Database_ = database;
    }

    void SetNextReplyCallback(NYdbGrpc::IRequestContextBase::TOnNextReply&& cb) override {
        NextReplyCallback_ = std::move(cb);
    }

    bool IsStreamCall() const override {
        return IsStreamCall_;
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
    ui32 LastStatus = 0;
    NYdbGrpc::IRequestContextBase::EStreamCtrl LastStreamCtrl = NYdbGrpc::IRequestContextBase::EStreamCtrl::CONT;
    grpc::StatusCode LastGrpcStatus = grpc::StatusCode::OK;
    TString LastErrorMessage;
    TString LastErrorDetails;

private:
    Ydb::Operations::CancelOperationRequest Request_;
    NYdbGrpc::TAuthState AuthState_{true};
    google::protobuf::Arena Arena_;
    bool IsStreamCall_ = false;
    THashMap<TString, TString> PeerMeta_;
    TVector<std::pair<TString, TString>> TrailingMetadata_;
    TString Database_;
    NYdbGrpc::IRequestContextBase::TOnNextReply NextReplyCallback_;
};

class TTestBiStreamContext
    : public NGRpcServer::IGRpcStreamingContext<Draft::Dummy::PingRequest, Draft::Dummy::PingResponse>
{
public:
    void Cancel() override {
        ++CancelCount;
    }

    void Attach(TActorId actor) override {
        ++AttachCount;
        AttachedActor = actor;
    }

    bool Read() override {
        ++ReadCount;
        return true;
    }

    bool Finish(const grpc::Status& status) override {
        ++FinishCount;
        LastGrpcStatus = status;
        return true;
    }

    NYdbGrpc::TAuthState& GetAuthState() const override {
        return AuthState_;
    }

    TString GetPeerName() const override {
        return "127.0.0.1";
    }

    TVector<TStringBuf> GetPeerMetaValues(TStringBuf key) const override {
        const auto it = PeerMeta_.find(TString(key));
        if (it == PeerMeta_.end()) {
            return {};
        }
        return {it->second};
    }

    grpc_compression_level GetCompressionLevel() const override {
        return GRPC_COMPRESS_LEVEL_NONE;
    }

    void UseDatabase(const TString& database) override {
        Database = database;
    }

    TString GetRpcMethodName() const override {
        return "Draft.Dummy.Ping";
    }

    bool Write(Draft::Dummy::PingResponse&&, const grpc::WriteOptions& = {}) override {
        ++WriteCount;
        return true;
    }

    bool WriteAndFinish(Draft::Dummy::PingResponse&&, const grpc::Status& status) override {
        ++WriteAndFinishCount;
        LastGrpcStatus = status;
        return true;
    }

    bool WriteAndFinish(Draft::Dummy::PingResponse&&, const grpc::WriteOptions&, const grpc::Status& status) override {
        ++WriteAndFinishWithOptionsCount;
        LastGrpcStatus = status;
        return true;
    }

public:
    ui32 CancelCount = 0;
    ui32 AttachCount = 0;
    ui32 ReadCount = 0;
    ui32 FinishCount = 0;
    ui32 WriteCount = 0;
    ui32 WriteAndFinishCount = 0;
    ui32 WriteAndFinishWithOptionsCount = 0;
    TActorId AttachedActor;
    grpc::Status LastGrpcStatus;
    TString Database;

private:
    mutable NYdbGrpc::TAuthState AuthState_{true};
    THashMap<TString, TString> PeerMeta_;
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
        "GrpcRequestProxy",
        NWilson::EFlags::NONE,
        runtime.GetActorSystem(0));
}

void DispatchReadyEvents(TTestActorRuntime& runtime) {
    runtime.DispatchEvents(TDispatchOptions(), TDuration::MilliSeconds(1));
}

} // namespace

Y_UNIT_TEST_SUITE(TestSetCloudPermissions) {

Y_UNIT_TEST(CanSetAllPermissions) {
    TTestSetup setup("user1", "/Root/db", {
        {"cloud_id", TTestSetup::CloudId},
        {"folder_id", TTestSetup::FolderId},
        {"database_id", TTestSetup::DatabaseId}
    });
    const TString userToken = "Bearer " + setup.UserSid;
    // Use TEvRequestAuthAndCheck to check permissions for gizmo resource
    std::unique_ptr<NGRpcService::TEvRequestAuthAndCheck> ev = std::make_unique<NGRpcService::TEvRequestAuthAndCheck>(
        setup.DbPath,
        TMaybe<TString>(userToken),
        setup.FakeMonActor,
        NGRpcService::TAuditMode::Modifying(NGRpcService::TAuditMode::TLogClassConfig::ClusterAdmin),
        "192.168.0.101");

    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    NGRpcService::TEvRequestAuthAndCheckResult* requestAuthAndCheckResultEv = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TEvRequestAuthAndCheckResult>(handle);
    UNIT_ASSERT_EQUAL(requestAuthAndCheckResultEv->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(requestAuthAndCheckResultEv->UserToken);
    UNIT_ASSERT_EQUAL_C(requestAuthAndCheckResultEv->UserToken->GetUserSID(), "user1@as", requestAuthAndCheckResultEv->UserToken->GetUserSID());
    UNIT_ASSERT_EQUAL_C(requestAuthAndCheckResultEv->UserToken->GetGroupSIDs().size(), 23, requestAuthAndCheckResultEv->UserToken->GetGroupSIDs().size());
    THashSet<TString> groups;
    for (const auto& p : requestAuthAndCheckResultEv->UserToken->GetGroupSIDs()) {
        groups.insert(p);
    }
    // Check that user has all permissions, check_actor sends all permissions to exam them
    for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(groups.contains(permission + "-" + TTestSetup::DatabaseId + "@as"), permission + "-" + TTestSetup::DatabaseId + "@as");
    }
    for (const auto& permission : TTestSetup::GizmoPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(groups.contains(permission + "-" + TTestSetup::GizmoId + "@as"), permission + "-" + TTestSetup::GizmoId + "@as");
    }
    for (const auto& permission : TTestSetup::ClusterPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
    }
}

Y_UNIT_TEST(CanSetPermissionsWithoutGizmoResourse) {
    TTestSetup setup("user1", "/Root/db", {
        {"cloud_id", TTestSetup::CloudId},
        {"folder_id", TTestSetup::FolderId},
        {"database_id", TTestSetup::DatabaseId}
    });
    const TString userToken = "Bearer " + setup.UserSid;
    // Use TRefreshTokenGenericRequest to simple initialize it
    std::unique_ptr<NGRpcService::TRefreshTokenGenericRequest> ev = std::make_unique<NGRpcService::TRefreshTokenGenericRequest>(
        userToken,
        setup.DbPath,
        setup.FakeMonActor);
    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    NGRpcService::TRefreshTokenGenericRequest* refreshTokenGenericRequestEv = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TRefreshTokenGenericRequest>(handle);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetAuthState().State, NYdbGrpc::TAuthState::AS_OK);
    UNIT_ASSERT(refreshTokenGenericRequestEv->GetInternalToken());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID(), "user1@as", refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size(), 19, refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size());
    THashSet<TString> groups;
    for (const auto& p : refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs()) {
        groups.insert(p);
    }
    // Check that user has all permissions, check_actor sends all permissions to exam them except for ydb.developerApi.get and ydb.developerApi.update
    for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(groups.contains(permission + "-" + TTestSetup::DatabaseId + "@as"), permission + "-" + TTestSetup::DatabaseId + "@as");
    }
    for (const auto& permission : TTestSetup::ClusterPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
    }
    // Check that permissions for gizmo resourse are absent
    for (const auto& permission : TTestSetup::GizmoPermissions) {
        UNIT_ASSERT_C(!groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::GizmoId + "@as"), permission + "-" + TTestSetup::GizmoId + "@as");
    }
}

Y_UNIT_TEST(CanSetPermissionsForRootDb) {
    TTestSetup setup("user1", "/Root", {
        {"cloud_id", TTestSetup::ClusterCloudId},
        {"folder_id", TTestSetup::ClusterFolderId},
    });
    const TString userToken = "Bearer " + setup.UserSid;
    // Use TRefreshTokenGenericRequest to simple initialize it
    std::unique_ptr<NGRpcService::TRefreshTokenGenericRequest> ev = std::make_unique<NGRpcService::TRefreshTokenGenericRequest>(
        userToken,
        setup.DbPath,
        setup.FakeMonActor);
    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    NGRpcService::TRefreshTokenGenericRequest* refreshTokenGenericRequestEv = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TRefreshTokenGenericRequest>(handle);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetAuthState().State, NYdbGrpc::TAuthState::AS_OK);
    UNIT_ASSERT(refreshTokenGenericRequestEv->GetInternalToken());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID(), "user1@as", refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size(), 12, refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size());
    THashSet<TString> groups;
    for (const auto& p : refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs()) {
        groups.insert(p);
    }
    // Check that user has all permissions, check_actor sends all permissions to exam them
    for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
        // Attribute database_id is not set for root db, user has not virtual group in format <permission>-<database_id>@as
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::DatabaseId + "@as"), permission + "-" + TTestSetup::DatabaseId + "@as");
    }
    for (const auto& permission : TTestSetup::ClusterPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
    }
    // Check that permissions for gizmo resourse are absent
    for (const auto& permission : TTestSetup::GizmoPermissions) {
        UNIT_ASSERT_C(!groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::GizmoId + "@as"), permission + "-" + TTestSetup::GizmoId + "@as");
    }
}

Y_UNIT_TEST(CanSetPermissionsForDbWithoutCloudUserAttributes) {
    TTestSetup setup("user1", "/Root/db", {
        {"test_attr_1", "111"},
        {"test_attr_2", "222"},
        {"test_attr_1", "333"}
    });
    const TString userToken = "Bearer " + setup.UserSid;
    // Use TRefreshTokenGenericRequest to simple initialize it
    std::unique_ptr<NGRpcService::TRefreshTokenGenericRequest> ev = std::make_unique<NGRpcService::TRefreshTokenGenericRequest>(
        userToken,
        setup.DbPath,
        setup.FakeMonActor);
    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    NGRpcService::TRefreshTokenGenericRequest* refreshTokenGenericRequestEv = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TRefreshTokenGenericRequest>(handle);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetAuthState().State, NYdbGrpc::TAuthState::AS_OK);
    UNIT_ASSERT(refreshTokenGenericRequestEv->GetInternalToken());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID(), "user1@as", refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size(), 5, refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size());
    THashSet<TString> groups;
    for (const auto& p : refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs()) {
        groups.insert(p);
    }
    // /Root/db has not user attributes associated with cloud resources cloud_id, folder_id, database_id
    for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
        UNIT_ASSERT_C(!groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::DatabaseId + "@as"), permission + "-" + TTestSetup::DatabaseId + "@as");
    }
    for (const auto& permission : TTestSetup::ClusterPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
    }
    // Check that permissions for gizmo resourse are absent
    for (const auto& permission : TTestSetup::GizmoPermissions) {
        UNIT_ASSERT_C(!groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::GizmoId + "@as"), permission + "-" + TTestSetup::GizmoId + "@as");
    }
}

} // CheckCloudPermissions

Y_UNIT_TEST_SUITE(TGrpcRequestCheckActorTracing) {

Y_UNIT_TEST(DoesNotFinishGrpcRequestProxySpanBeforeRuntimeEventPass) {
    TTestSetup setup("user1", "/Root/db", {
        {"cloud_id", TTestSetup::CloudId},
        {"folder_id", TTestSetup::FolderId},
        {"database_id", TTestSetup::DatabaseId}
    });
    const TString userToken = "Bearer " + setup.UserSid;

    std::unique_ptr<NGRpcService::TEvProxyRuntimeEvent> ev = std::make_unique<TTestProxyRuntimeEvent>(
        setup.DbPath,
        userToken,
        setup.FakeMonActor);

    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    auto* result = setup.GetRuntime()->GrabEdgeEvent<TEvRuntimeRequestPassed>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->FinishSpanCalls, 0);
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanForAuthAndCheckRequest) {
    TTestSetup setup("user1", "/Root/db", {
        {"cloud_id", TTestSetup::CloudId},
        {"folder_id", TTestSetup::FolderId},
        {"database_id", TTestSetup::DatabaseId}
    });
    auto* uploader = SetupFakeWilsonUploader(*setup.GetRuntime());
    const TString userToken = "Bearer " + setup.UserSid;

    std::unique_ptr<NGRpcService::TEvRequestAuthAndCheck> ev = std::make_unique<NGRpcService::TEvRequestAuthAndCheck>(
        setup.DbPath,
        TMaybe<TString>(userToken),
        setup.FakeMonActor,
        NGRpcService::TAuditMode::Modifying(NGRpcService::TAuditMode::TLogClassConfig::ClusterAdmin),
        "192.168.0.101");
    ev->StartTracing(MakeGrpcRequestProxySpan(*setup.GetRuntime()));

    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    auto* result = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TEvRequestAuthAndCheckResult>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(*setup.GetRuntime());
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
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
    request.ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(SecondFinishSpanDoesNotEmitAnotherWilsonSpan) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(runtime);
    request.FinishSpan();
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(ManualFinishSpanEmitsGrpcRequestProxySpan) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.FinishSpan();
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnRpcStatusError) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.ReplyWithRpcStatus(grpc::StatusCode::UNAVAILABLE, "unavailable", "details");
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyErrorCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnUnauthenticatedReply) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.ReplyUnauthenticated("unauthenticated");
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyUnauthenticatedCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnGrpcError) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.ReplyGrpcError(grpc::StatusCode::INTERNAL, "internal", "details");
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyErrorCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnUnarySerializedResult) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.SendSerializedResult(TString("serialized-result"), Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ByteReplyCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnUnarySerializedRopeResult) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.SendSerializedResult(TRope(TString("serialized-rope-result")), Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ByteReplyCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnFinishStream) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestGrpcRequestContext>();
    TTestGrpcRequest request(ctx.Get(), [](std::unique_ptr<NGRpcService::IRequestNoOpCtx>, const NGRpcService::IFacilityProvider&) {});

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.FinishStream(Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->FinishStreamingOkCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
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
    request.ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(runtime);

    UNIT_ASSERT(delayedReply);
    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 0);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 0);

    delayedReply->Pass();
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->ReplyCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
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
    const bool valid = TPublicProxyHandleMethods::ValidateAndReplyOnError(&request);
    DispatchReadyEvents(runtime);

    UNIT_ASSERT(!valid);
    UNIT_ASSERT_VALUES_EQUAL(ctx->AttachCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(ctx->WriteAndFinishCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnUnauthenticatedReply) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    NGRpcService::TEvBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.ReplyUnauthenticated("unauthenticated");
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->FinishCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnFinish) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    NGRpcService::TEvBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.Finish(Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->FinishCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnWriteAndFinish) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    NGRpcService::TEvBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.WriteAndFinish(Draft::Dummy::PingResponse(), Ydb::StatusIds::SUCCESS);
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->WriteAndFinishCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

Y_UNIT_TEST(FinishesGrpcRequestProxySpanOnWriteAndFinishWithOptions) {
    TTestActorRuntime runtime;
    runtime.Initialize(TAppPrepare().Unwrap());
    auto* uploader = SetupFakeWilsonUploader(runtime);
    auto ctx = MakeIntrusive<TTestBiStreamContext>();
    NGRpcService::TEvBiStreamPingRequest request(ctx);

    request.StartTracing(MakeGrpcRequestProxySpan(runtime));
    request.WriteAndFinish(Draft::Dummy::PingResponse(), Ydb::StatusIds::SUCCESS, grpc::WriteOptions());
    DispatchReadyEvents(runtime);

    UNIT_ASSERT_VALUES_EQUAL(ctx->WriteAndFinishWithOptionsCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(uploader->Spans[0].name(), "GrpcRequestProxy");
}

} // TGrpcRequestBiStreamTracing
