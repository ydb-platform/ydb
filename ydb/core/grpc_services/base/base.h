#pragma once

#include "iface.h"

#include <grpc++/support/byte_buffer.h>
#include <grpc++/support/slice.h>

#include <ydb/library/grpc/server/grpc_request_base.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/api/protos/ydb_common.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>
#include <ydb/library/aclib/aclib.h>

#include <ydb/core/jaeger_tracing/request_discriminator.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>
#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/base/events.h>

#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/stream/str.h>

namespace NKikimr {

namespace NSchemeCache {
    struct TSchemeCacheNavigate;
}

namespace NRpcService {
    struct TRlPath {
        TString CoordinationNode;
        TString ResourcePath;
    };
}

namespace NGRpcService {

using TYdbIssueMessageType = Ydb::Issue::IssueMessage;

std::pair<TString, TString> SplitPath(const TMaybe<TString>& database, const TString& path);
std::pair<TString, TString> SplitPath(const TString& path);

inline TActorId CreateGRpcRequestProxyId(int n = 0) {
    if (n == 0) {
        const auto actorId = TActorId(0, "GRpcReqProxy");
        return actorId;
    }

    const auto actorId = TActorId(0, TStringBuilder() << "GRpcReqPro" << n);
    return actorId;
}

struct TRpcServices {
    enum EServiceId {
        EvMakeDirectory = EventSpaceBegin(TKikimrEvents::ES_GRPC_CALLS),
        EvRemoveDirectory,
        EvAlterTable,
        EvCreateTable,
        EvDropTable,
        EvCopyTable,
        EvCopyTables,
        EvListDirectory,
        EvRenameTables,
        EvDescribeTable,
        EvDescribePath,
        EvGetOperation,
        EvCreateSession,
        EvDeleteSession,
        EvKeepAlive = EvDeleteSession + 3,
        EvReadTable = EvKeepAlive + 3,
        EvGrpcStreamIsReady,
        EvExplainDataQuery,
        EvPrepareDataQuery,
        EvExecuteDataQuery,
        EvExecuteSchemeQuery,
        EvCreateTenant,
        EvAlterTenant,
        EvGetTenantStatus,
        EvListTenants,
        EvRemoveTenant,
        EvBeginTransaction,
        EvCommitTransaction,
        EvRollbackTransaction,
        EvModifyPermissions,
        EvListEndpoints,
        EvDescribeTenantOptions,
        EvDescribeTableOptions,
        EvCreateCoordinationNode,
        EvAlterCoordinationNode,
        EvDropCoordinationNode,
        EvDescribeCoordinationNode,
        EvCancelOperation,
        EvForgetOperation,
        EvExecDataQueryAst,
        EvExecuteYqlScript,
        EvUploadRows,
        EvS3Listing,
        EvExplainDataQueryAst,
        EvReadColumns,
        EvBiStreamPing,
        EvRefreshToken, // internal call, pair to EvStreamWriteRefreshToken
        EvGetShardLocations,
        EvExperimentalStreamQuery,
        EvStreamPQWrite,
        EvStreamPQMigrationRead,
        EvStreamTopicWrite,
        EvStreamTopicRead,
        EvStreamTopicDirectRead,
        EvPQReadInfo,
        EvTopicCommitOffset,
        EvListOperations,
        EvExportToYt,
        EvDiscoverPQClusters,
        EvListFederationDatabases,
        EvBulkUpsert,
        EvWhoAmI,
        EvKikhouseDescribeTable,
        EvCreateRateLimiterResource,
        EvAlterRateLimiterResource,
        EvDropRateLimiterResource,
        EvListRateLimiterResources,
        EvDescribeRateLimiterResource,
        EvAcquireRateLimiterResource,
        EvKikhouseCreateSnapshot,
        EvKikhouseRefreshSnapshot,
        EvKikhouseDiscardSnapshot,
        EvExportToS3,
        EvSelfCheck,
        EvStreamExecuteScanQuery,
        EvPQDropTopic,
        EvPQCreateTopic,
        EvPQAlterTopic,
        EvPQDescribeTopic,
        EvPQAddReadRule,
        EvPQRemoveReadRule,
        EvDropTopic,
        EvCreateTopic,
        EvAlterTopic,
        EvDescribeTopic,
        EvDescribeConsumer,
        EvDescribePartition,
        EvGetDiskSpaceUsage,
        EvStopServingDatabase,
        EvCoordinationSession,
        EvImportFromS3,
        EvLongTxBegin,
        EvLongTxCommit,
        EvLongTxRollback,
        EvLongTxWrite,
        EvLongTxRead,
        EvExplainYqlScript,
        EvImportData,
        EvAnalyticsReserved,
        EvDataStreamsCreateStream,
        EvDataStreamsDeleteStream,
        EvDataStreamsDescribeStream,
        EvDataStreamsRegisterStreamConsumer,
        EvDataStreamsDeregisterStreamConsumer,
        EvDataStreamsDescribeStreamConsumer,
        EvDataStreamsListStreams,
        EvDataStreamsListShards,
        EvDataStreamsPutRecord,
        EvDataStreamsPutRecords,
        EvDataStreamsGetRecords,
        EvDataStreamsGetShardIterator,
        EvDataStreamsSubscribeToShard,
        EvDataStreamsDescribeLimits,
        EvDataStreamsDescribeStreamSummary,
        EvDataStreamsDecreaseStreamRetentionPeriod,
        EvDataStreamsIncreaseStreamRetentionPeriod,
        EvDataStreamsUpdateShardCount,
        EvDataStreamsUpdateStream,
        EvDataStreamsSetWriteQuota,
        EvDataStreamsListStreamConsumers,
        EvDataStreamsAddTagsToStream,
        EvDataStreamsDisableEnhancedMonitoring,
        EvDataStreamsEnableEnhancedMonitoring,
        EvDataStreamsListTagsForStream,
        EvDataStreamsMergeShards,
        EvDataStreamsRemoveTagsFromStream,
        EvDataStreamsSplitShard,
        EvDataStreamsStartStreamEncryption,
        EvDataStreamsStopStreamEncryption,
        EvStreamExecuteYqlScript,
        EvYandexQueryCreateQuery,
        EvYandexQueryListQueries,
        EvYandexQueryDescribeQuery,
        EvYandexQueryGetQueryStatus,
        EvYandexQueryModifyQuery,
        EvYandexQueryDeleteQuery,
        EvYandexQueryControlQuery,
        EvYandexQueryGetResultData,
        EvYandexQueryListJobs,
        EvYandexQueryDescribeJob,
        EvYandexQueryCreateConnection,
        EvYandexQueryListConnections,
        EvYandexQueryDescribeConnection,
        EvYandexQueryModifyConnection,
        EvYandexQueryDeleteConnection,
        EvYandexQueryTestConnection,
        EvYandexQueryCreateBinding,
        EvYandexQueryListBindings,
        EvYandexQueryDescribeBinding,
        EvYandexQueryModifyBinding,
        EvYandexQueryDeleteBinding,
        EvCreateLogStore,
        EvDescribeLogStore,
        EvDropLogStore,
        EvCreateLogTable,
        EvDescribeLogTable,
        EvDropLogTable,
        EvAlterLogTable,
        EvLogin,
        EvAnalyticsInternalPingTask,
        EvAnalyticsInternalGetTask,
        EvAnalyticsInternalWriteTaskResult,
        EvAnalyticsInternalNodesHealthCheck,
        EvCreateYndxRateLimiterResource,
        EvAlterYndxRateLimiterResource,
        EvDropYndxRateLimiterResource,
        EvListYndxRateLimiterResources,
        EvDescribeYndxRateLimiterResource,
        EvAcquireYndxRateLimiterResource,
        EvGrpcRuntimeRequest,
        EvNodeCheckRequest,
        EvStreamWriteRefreshToken,    // internal call, pair to EvRefreshToken
        EvRequestAuthAndCheck, // performs authorization and runs GrpcRequestCheckActor
        EvRequestAuthAndCheckResult,
        // !!! DO NOT ADD NEW REQUEST !!!
    };

    struct TEvGrpcNextReply : public TEventLocal<TEvGrpcNextReply, TRpcServices::EvGrpcStreamIsReady> {
        TEvGrpcNextReply(size_t left)
            : LeftInQueue(left)
        {}
        const size_t LeftInQueue;
    };

    struct TEvCancelOperation : public TEventLocal<TEvCancelOperation, TRpcServices::EvCancelOperation> {};
    struct TEvForgetOperation : public TEventLocal<TEvForgetOperation, TRpcServices::EvForgetOperation> {};
};

// Should be specialized for real responses
template <class T>
void FillYdbStatus(T& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status);

class TProtoResponseHelper {
public:
    template <typename T, typename C>
    static void SendProtoResponse(const T& r, Ydb::StatusIds::StatusCode status, C& ctx) {
        T* resp = google::protobuf::Arena::CreateMessage<T>(ctx->GetArena());
        resp->CopyFrom(r);
        ctx->Reply(resp, status);
    }
};

class IRequestCtxBase : public virtual IRequestCtxBaseMtSafe {
public:
    virtual ~IRequestCtxBase() = default;
    // Returns true if client has the specified capability
    virtual bool HasClientCapability(const TString& capability) const = 0;
    // Reply using YDB status code
    virtual void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) = 0;
    // Reply using "transport error code"
    virtual void ReplyWithRpcStatus(grpc::StatusCode code, const TString& msg = "", const TString& details = "") = 0;
    // Return address of the peer
    virtual TString GetPeerName() const = 0;
    // Return deadile of request execution, calculated from client timeout by grpc
    virtual TInstant GetDeadline() const = 0;
    // Meta value from request
    virtual const TMaybe<TString> GetPeerMetaValues(const TString&) const = 0;
    // Auth property from connection
    virtual TVector<TStringBuf> FindClientCert() const = 0;
    // Returns path and resource for rate limiter
    virtual TMaybe<NRpcService::TRlPath> GetRlPath() const = 0;
    // Raise issue on the context
    virtual void RaiseIssue(const NYql::TIssue& issue) = 0;
    virtual void RaiseIssues(const NYql::TIssues& issues) = 0;
    virtual const TString& GetRequestName() const = 0;
    virtual void SetDiskQuotaExceeded(bool disk) = 0;
    virtual bool GetDiskQuotaExceeded() const = 0;

    virtual void AddAuditLogPart(const TStringBuf& name, const TString& value) = 0;
    virtual const TAuditLogParts& GetAuditLogParts() const = 0;
};

class TRespHookCtx : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TRespHookCtx>;
    using TContextPtr = TIntrusivePtr<NYdbGrpc::IRequestContextBase>;
    using TMessage = NProtoBuf::Message;

    TRespHookCtx(TContextPtr ctx, TMessage* data, const TString& requestName, ui64 ru, ui32 status)
        : Ctx_(ctx)
        , RespData_(data)
        , RequestName_(requestName)
        , Ru_(ru)
        , Status_(status)
    {}

    void Pass() {
        Ctx_->Reply(RespData_, Status_);
    }

    ui64 GetConsumedRu() const {
        return Ru_;
    }

    const TString& GetRequestName() const {
        return RequestName_;
    }

private:
    TIntrusivePtr<NYdbGrpc::IRequestContextBase> Ctx_;
    TMessage* RespData_; //Allocated on arena owned by implementation of IRequestContextBase
    const TString RequestName_;
    const ui64 Ru_;
    const ui32 Status_;
};

using TRespHook = std::function<void(TRespHookCtx::TPtr ctx)>;

enum class TRateLimiterMode : ui8 {
    Off = 0,
    Rps = 1,
    Ru = 2,
    RuOnProgress = 3,
    RuManual = 4,
};

#define RLSWITCH(mode) \
    IsRlAllowed() ? mode : TRateLimiterMode::Off

enum class TAuditMode : bool {
    Off = false,
    Auditable = true,
};

class ICheckerIface;

// The way to pass some common data to request processing
class IFacilityProvider {
public:
    virtual ui64 GetChannelBufferSize() const = 0;
    // Registers new actor using method chosen by grpc proxy
    virtual TActorId RegisterActor(IActor* actor) const = 0;
};

struct TRequestAuxSettings {
    TRateLimiterMode RlMode = TRateLimiterMode::Off;
    void (*CustomAttributeProcessor)(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData, ICheckerIface*) = nullptr;
    TAuditMode AuditMode = TAuditMode::Off;
    NJaegerTracing::ERequestType RequestType = NJaegerTracing::ERequestType::UNSPECIFIED;
};

// grpc_request_proxy part
// The interface is used to perform authentication and check database access right
class IRequestProxyCtx : public virtual IRequestCtxBase {
public:
    virtual ~IRequestProxyCtx() = default;

    // auth
    virtual const TMaybe<TString> GetYdbToken() const = 0;
    virtual void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) = 0;
    virtual void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>& token) = 0;
    virtual const NYdbGrpc::TAuthState& GetAuthState() const = 0;
    virtual void ReplyUnauthenticated(const TString& msg = "") = 0;
    virtual void ReplyUnavaliable() = 0;
    virtual TVector<TStringBuf> FindClientCertPropertyValues() const = 0;

    // tracing
    virtual void StartTracing(NWilson::TSpan&& span) = 0;
    virtual void FinishSpan() = 0;
    // Returns pointer to a state that denotes whether this request ever been a subject
    // to tracing decision. CAN be nullptr
    virtual bool* IsTracingDecided() = 0;

    // Used for per-type sampling
    virtual NJaegerTracing::TRequestDiscriminator GetRequestDiscriminator() const {
        return NJaegerTracing::TRequestDiscriminator::EMPTY;
    };

    // validation
    virtual bool Validate(TString& error) = 0;

    // counters
    virtual void SetCounters(IGRpcProxyCounters::TPtr counters) = 0;
    virtual IGRpcProxyCounters::TPtr GetCounters() const = 0;
    virtual void UseDatabase(const TString& database) = 0;

    // rate limiting

    // This method allows to set hook for unary call.
    // The hook will be called at the reply time
    // TRespHookCtx::Ptr will be passed in to the hook, it is allow
    // to store the ctx somwhere to delay reply and then call Pass() to send response.
    virtual void SetRespHook(TRespHook&& hook) = 0;
    virtual void SetRlPath(TMaybe<NRpcService::TRlPath>&& path) = 0;
    virtual TRateLimiterMode GetRlMode() const = 0;
    virtual bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData,
        ICheckerIface* iface) = 0;

    // Pass request for next processing
    virtual void Pass(const IFacilityProvider& facility) = 0;

    // audit
    virtual bool IsAuditable() const {
        return false;
    }
    virtual void SetAuditLogHook(TAuditLogHook&& hook) = 0;
};

// Request context
// The interface is used for rpc_ request actors
class IRequestCtx
    : public virtual IRequestCtxMtSafe
    , public virtual IRequestCtxBase
{
    friend class TProtoResponseHelper;
public:
    using EStreamCtrl = NYdbGrpc::IRequestContextBase::EStreamCtrl;
    virtual google::protobuf::Message* GetRequestMut() = 0;

    virtual void SetRuHeader(ui64 ru) = 0;
    virtual void AddServerHint(const TString& hint) = 0;
    virtual void SetCostInfo(float consumed_units) = 0;

    virtual void SetStreamingNotify(NYdbGrpc::IRequestContextBase::TOnNextReply&& cb) = 0;
    virtual void FinishStream(ui32 status) = 0;

    virtual void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, EStreamCtrl flag = EStreamCtrl::CONT) = 0;

    virtual void Reply(NProtoBuf::Message* resp, ui32 status = 0) = 0;

protected:
    virtual void FinishRequest() = 0;
};

class IRequestOpCtx : public IRequestCtx {
public:
    virtual void SendOperation(const Ydb::Operations::Operation& operation) = 0;
    virtual void SendResult(const google::protobuf::Message& result,
        Ydb::StatusIds::StatusCode status) = 0;
    // Legacy, do not use for modern code
    virtual void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message) = 0;
    // Legacy, do not use for modern code
    virtual void SendResult(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message) = 0;
};

class IRequestNoOpCtx : public IRequestCtx {
};

struct TCommonResponseFillerImpl {
    template <typename T>
    static void FillImpl(T& resp, const NYql::TIssues& issues, Ydb::StatusIds::StatusCode status) {
        resp.set_status(status);
        NYql::IssuesToMessage(issues, resp.mutable_issues());
    }
};

template <typename TResp, bool IsOperation = true>
struct TCommonResponseFiller : private TCommonResponseFillerImpl {
    static void Fill(TResp& resp, const NYql::TIssues& issues, Ydb::CostInfo* costInfo, Ydb::StatusIds::StatusCode status) {
        auto& operation = *resp.mutable_operation();
        operation.set_ready(true);
        if (costInfo) {
            operation.mutable_cost_info()->Swap(costInfo);
        }
        FillImpl(operation, issues, status);
    }
};

template <typename TResp>
struct TCommonResponseFiller<TResp, false> : private TCommonResponseFillerImpl {
    static void Fill(TResp& resp, const NYql::TIssues& issues, Ydb::CostInfo*, Ydb::StatusIds::StatusCode status) {
        FillImpl(resp, issues, status);
    }
};

template <ui32 TRpcId>
class TRefreshTokenImpl
    : public IRequestProxyCtx
    , public TEventLocal<TRefreshTokenImpl<TRpcId>, TRpcId>
{
public:
    TRefreshTokenImpl(const TString& token, const TString& database, TActorId from)
        : Token_(token)
        , Database_(database)
        , From_(from)
        , State_(true)
    { }

    const TMaybe<TString> GetYdbToken() const override {
        return Token_;
    }

    void StartTracing(NWilson::TSpan&& /*span*/) override {}
    void FinishSpan() override {}
    bool* IsTracingDecided() override {
        return nullptr;
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        State_.State = state;
    }

    void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        InternalToken_ = token;
    }

    bool HasClientCapability(const TString&) const override {
        return false;
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return Database_;
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        return State_;
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return InternalToken_;
    }

    const TString& GetSerializedToken() const override {
        if (InternalToken_) {
            return InternalToken_->GetSerializedToken();
        }

        return EmptySerializedTokenMessage_;
    }

    TString GetPeerName() const override {
        return {};
    }

    void SetRlPath(TMaybe<NRpcService::TRlPath>&&) override {
    }

    bool IsClientLost() const override {
        return false;
    }

    TMaybe<NRpcService::TRlPath> GetRlPath() const override {
        return Nothing();
    }

    const TMaybe<TString> GetPeerMetaValues(const TString&) const override {
        return {};
    }

    TVector<TStringBuf> FindClientCert() const override {
        Y_ABORT("Unimplemented");
        return {};
    }

    void SetDiskQuotaExceeded(bool) override {
    }

    bool GetDiskQuotaExceeded() const override {
        return false;
    }

    void ReplyWithRpcStatus(grpc::StatusCode, const TString&, const TString&) override {
        Y_ABORT("Unimplemented");
    }

    void ReplyUnauthenticated(const TString&) override;
    void ReplyUnavaliable() override;
    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        switch (status) {
            case Ydb::StatusIds::UNAVAILABLE:
                return ReplyUnavaliable();
            case Ydb::StatusIds::UNAUTHORIZED:
                RaiseIssue(NYql::TIssue{"got UNAUTHORIZED ydb status"});
                return ReplyUnauthenticated(TString());
            default: {
                auto ss = TStringBuilder() << "got unexpected: " << status << "ydb status";
                RaiseIssue(NYql::TIssue{ss});
                return ReplyUnavaliable();
            }
        }
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        IssueManager_.RaiseIssue(issue);
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        IssueManager_.RaiseIssues(issues);
    }

    bool Validate(TString&) override {
        return true;
    }

    void SetCounters(IGRpcProxyCounters::TPtr) override {
    }

    IGRpcProxyCounters::TPtr GetCounters() const override {
        return nullptr;
    }

    void UseDatabase(const TString& database) override {
        Y_UNUSED(database);
    }

    TVector<TStringBuf> FindClientCertPropertyValues() const override {
        return {};
    }

    TActorId GetFromId() const {
        return From_;
    }

    const TString& GetRequestName() const override {
        static TString str = "refresh token internal request";
        return str;
    }

    TMaybe<TString> GetTraceId() const override {
        return {};
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return {};
    }

    TMaybe<TString> GetSdkBuildInfo() const {
        return {};
    }

    TMaybe<TString> GetGrpcUserAgent() const {
        return {};
    }

    TInstant GetDeadline() const override {
        return TInstant::Max();
    }

    void SetRespHook(TRespHook&&) override { /* do nothing */}

    TRateLimiterMode GetRlMode() const override {
        return TRateLimiterMode::Off;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult&, ICheckerIface*) override {
        return false;
    }

    void Pass(const IFacilityProvider&) override {
        Y_ABORT("unimplemented");
    }

    void SetAuditLogHook(TAuditLogHook&&) override {
        Y_ABORT("unimplemented for TRefreshTokenImpl");
    }

    // IRequestCtxBase
    //
    void AddAuditLogPart(const TStringBuf&, const TString&) override {
        Y_ABORT("unimplemented for TRefreshTokenImpl");
    }
    const TAuditLogParts& GetAuditLogParts() const override {
        Y_ABORT("unimplemented for TRefreshTokenImpl");
    }

private:
    const TString Token_;
    const TString Database_;
    const TActorId From_;
    NYdbGrpc::TAuthState State_;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken_;
    inline static const TString EmptySerializedTokenMessage_;
    NYql::TIssueManager IssueManager_;
};

namespace {

    inline TMaybe<TString> ToMaybe(const TVector<TStringBuf>& vec) {
        if (vec.empty()) {
            return {};
        }
        return TString{vec[0]};
    }

    inline const TMaybe<TString> ExtractYdbToken(const TVector<TStringBuf>& authHeadValues) {
        if (authHeadValues.empty()) {
            return {};
        }
        return TString{authHeadValues[0]};
    }

    inline const TMaybe<TString> ExtractDatabaseName(const TVector<TStringBuf>& dbHeaderValues) {
        if (dbHeaderValues.empty()) {
            return {};
        }
        return CGIUnescapeRet(dbHeaderValues[0]);
    }

    inline TString MakeAuthError(const TString& in, NYql::TIssueManager& issues) {
        TStringStream out;
        out << "unauthenticated"
            << (in ? ", " : "") << in
            << (issues.GetIssues() ? ": " : "");
        issues.GetIssues().PrintTo(out, true /* one line */);
        return out.Str();
    }

}

template <ui32 TRpcId, typename TReq, typename TResp, TRateLimiterMode RlMode = TRateLimiterMode::Off>
class TGRpcRequestBiStreamWrapper
    : public IRequestProxyCtx
    , public TEventLocal<TGRpcRequestBiStreamWrapper<TRpcId, TReq, TResp, RlMode>, TRpcId>
{
public:
    using TRequest = TReq;
    using TResponse = TResp;
    using IStreamCtx = NGRpcServer::IGRpcStreamingContext<TRequest, TResponse>;
    static constexpr TRateLimiterMode RateLimitMode = RlMode;

    TGRpcRequestBiStreamWrapper(TIntrusivePtr<IStreamCtx> ctx, bool rlAllowed = true)
        : Ctx_(ctx)
        , RlAllowed_(rlAllowed)
    { }

    bool IsClientLost() const override {
        // TODO: Implement for BiDirectional streaming
        return false;
    }

    TRateLimiterMode GetRlMode() const override {
        return RlAllowed_ ? RateLimitMode : TRateLimiterMode::Off;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult&, ICheckerIface*) override {
        return false;
    }

    const TMaybe<TString> GetYdbToken() const override {
        return ExtractYdbToken(Ctx_->GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER));
    }

    bool HasClientCapability(const TString& capability) const override {
        return FindPtr(Ctx_->GetPeerMetaValues(NYdb::YDB_CLIENT_CAPABILITIES), capability);
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return ExtractDatabaseName(Ctx_->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER));
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        auto& s = Ctx_->GetAuthState();
        s.State = state;
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        return Ctx_->GetAuthState();
    }

    void ReplyWithRpcStatus(grpc::StatusCode, const TString&, const TString&) override {
        Y_ABORT("Unimplemented");
    }

    void ReplyUnauthenticated(const TString& in) override {
        Ctx_->Finish(grpc::Status(grpc::StatusCode::UNAUTHENTICATED, MakeAuthError(in, IssueManager_)));
    }

    void ReplyUnavaliable() override {
        Ctx_->Attach(TActorId());
        TResponse resp;
        FillYdbStatus(resp, IssueManager_.GetIssues(), Ydb::StatusIds::UNAVAILABLE);
        Ctx_->WriteAndFinish(std::move(resp), grpc::Status::OK);
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        Ctx_->Attach(TActorId());
        TResponse resp;
        FillYdbStatus(resp, IssueManager_.GetIssues(), status);
        Ctx_->WriteAndFinish(std::move(resp), grpc::Status::OK);
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        IssueManager_.RaiseIssue(issue);
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        IssueManager_.RaiseIssues(issues);
    }

    void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        InternalToken_ = token;
    }

    void SetRlPath(TMaybe<NRpcService::TRlPath>&& path) override {
        RlPath_ = std::move(path);
    }

    TMaybe<NRpcService::TRlPath> GetRlPath() const override {
        return RlPath_;
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return InternalToken_;
    }

    const TString& GetSerializedToken() const override {
        if (InternalToken_) {
            return InternalToken_->GetSerializedToken();
        }

        return EmptySerializedTokenMessage_;
    }

    TString GetPeerName() const override {
        return Ctx_->GetPeerName();
    }

    bool Validate(TString&) override {
        return true;
    }

    void SetCounters(IGRpcProxyCounters::TPtr counters) override {
        Counters_ = counters;
    }

    IGRpcProxyCounters::TPtr GetCounters() const override {
        return Counters_;
    }

    void UseDatabase(const TString& database) override {
        Ctx_->UseDatabase(database);
    }

    TVector<TStringBuf> FindClientCertPropertyValues() const override {
        return {};
    }

    IStreamCtx* GetStreamCtx() {
        return Ctx_.Get();
    }

    const TString& GetRequestName() const override {
        return TRequest::descriptor()->name();
    }

    TMaybe<TString> GetTraceId() const override {
        return GetPeerMetaValues(NYdb::YDB_TRACE_ID_HEADER);
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return Span_.GetTraceId();
    }

    const TMaybe<TString> GetSdkBuildInfo() const {
        return GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    }

    const TMaybe<TString> GetRequestType() const {
        return GetPeerMetaValues(NYdb::YDB_REQUEST_TYPE_HEADER);
    }

    TInstant GetDeadline() const override {
        return TInstant::Max();
    }

    const TMaybe<TString> GetGrpcUserAgent() const {
        return GetPeerMetaValues(NYdbGrpc::GRPC_USER_AGENT_HEADER);
    }

    const TMaybe<TString> GetPeerMetaValues(const TString& key) const override {
        return ToMaybe(Ctx_->GetPeerMetaValues(key));
    }

    TVector<TStringBuf> FindClientCert() const override {
        Y_ABORT("Unimplemented");
        return {};
    }

    void SetDiskQuotaExceeded(bool) override {
    }

    bool GetDiskQuotaExceeded() const override {
        return false;
    }

    void RefreshToken(const TString& token, const TActorContext& ctx, TActorId id);

    void SetRespHook(TRespHook&&) override {
        /* cannot add hook to bidirect streaming */
        Y_ABORT("Unimplemented");
    }

    void Pass(const IFacilityProvider&) override {
        Y_ABORT("unimplemented");
    }

    void SetAuditLogHook(TAuditLogHook&&) override {
        Y_ABORT("unimplemented for TGRpcRequestBiStreamWrapper");
    }

    // IRequestProxyCtx
    //
    void StartTracing(NWilson::TSpan&& span) override {
        Span_ = std::move(span);
    }

    void FinishSpan() override {
        Span_.End();
    }

    bool* IsTracingDecided() override {
        return &IsTracingDecided_;
    }

    // IRequestCtxBase
    //
    void AddAuditLogPart(const TStringBuf&, const TString&) override {
        Y_ABORT("unimplemented for TGRpcRequestBiStreamWrapper");
    }
    const TAuditLogParts& GetAuditLogParts() const override {
        Y_ABORT("unimplemented for TGRpcRequestBiStreamWrapper");
    }

private:
    TIntrusivePtr<IStreamCtx> Ctx_;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken_;
    inline static const TString EmptySerializedTokenMessage_;
    NYql::TIssueManager IssueManager_;
    TMaybe<NRpcService::TRlPath> RlPath_;
    bool RlAllowed_;
    IGRpcProxyCounters::TPtr Counters_;
    NWilson::TSpan Span_;
    bool IsTracingDecided_ = false;
};

template <typename TDerived>
class TGrpcResponseSenderImpl : public IRequestOpCtx {
public:
    // IRequestOpCtx
    //
    void SendOperation(const Ydb::Operations::Operation& operation) override {
        auto self = Derived();
        if (operation.ready()) {
            self->FinishRequest();
        }
        auto resp = self->CreateResponseMessage();
        resp->mutable_operation()->CopyFrom(operation);
        self->Reply(resp, operation.status());
    }

    void SendResult(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message) override
    {
        auto self = Derived();
        self->FinishRequest();
        auto resp = self->CreateResponseMessage();
        auto deferred = resp->mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        deferred->mutable_issues()->MergeFrom(message);
        if (self->CostInfo) {
            deferred->mutable_cost_info()->Swap(self->CostInfo);
        }
        self->Reply(resp, status);
    }

    void SendResult(const google::protobuf::Message& result,
        Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message) override
    {
        auto self = Derived();
        self->FinishRequest();
        auto resp = self->CreateResponseMessage();
        auto deferred = resp->mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        deferred->mutable_issues()->MergeFrom(message);
        if (self->CostInfo) {
            deferred->mutable_cost_info()->Swap(self->CostInfo);
        }
        auto data = deferred->mutable_result();
        data->PackFrom(result);
        self->Reply(resp, status);
    }

    void SendResult(const google::protobuf::Message& result, Ydb::StatusIds::StatusCode status) override {
        auto self = Derived();
        self->FinishRequest();
        auto resp = self->CreateResponseMessage();
        auto deferred = resp->mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(status);
        if (self->CostInfo) {
            deferred->mutable_cost_info()->Swap(self->CostInfo);
        }
        NYql::IssuesToMessage(self->IssueManager.GetIssues(), deferred->mutable_issues());
        auto data = deferred->mutable_result();
        data->PackFrom(result);
        self->Reply(resp, status);
    }

private:
    TDerived* Derived() noexcept {
        return static_cast<TDerived*>(this);
    }
};

class TEvProxyRuntimeEvent
    : public IRequestProxyCtx
    , public TEventLocal<TEvProxyRuntimeEvent, TRpcServices::EvGrpcRuntimeRequest>
{
public:
    const TMaybe<TString> GetSdkBuildInfo() const {
        return GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    }

    const TMaybe<TString> GetGrpcUserAgent() const {
        return GetPeerMetaValues(NYdbGrpc::GRPC_USER_AGENT_HEADER);
    }
};

template <ui32 TRpcId, typename TDerived>
class TEvProxyLegacyEvent
    : public IRequestProxyCtx
    , public TEventLocal<TDerived, TRpcId>
{
public:
    const TMaybe<TString> GetSdkBuildInfo() const {
        return GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    }

    const TMaybe<TString> GetGrpcUserAgent() const {
        return GetPeerMetaValues(NYdbGrpc::GRPC_USER_AGENT_HEADER);
    }
};

template <ui32 TRpcId, typename TReq, typename TResp, bool IsOperation, typename TDerived>
class TGRpcRequestWrapperImpl
    : public std::conditional_t<IsOperation,
        TGrpcResponseSenderImpl<TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation, TDerived>>,
        IRequestNoOpCtx>
    , public std::conditional_t<TRpcId == TRpcServices::EvGrpcRuntimeRequest,
        TEvProxyRuntimeEvent,
        TEvProxyLegacyEvent<TRpcId, TDerived>>
{
    friend class TProtoResponseHelper;
    friend class TGrpcResponseSenderImpl<TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation, TDerived>>;

public:
    using TRequest = TReq;
    using TResponse = TResp;

    using TFinishWrapper = std::function<void(const NYdbGrpc::IRequestContextBase::TAsyncFinishResult&)>;

    TGRpcRequestWrapperImpl(NYdbGrpc::IRequestContextBase* ctx)
        : Ctx_(ctx)
    { }

    const TMaybe<TString> GetYdbToken() const override {
        return ExtractYdbToken(Ctx_->GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER));
    }

    bool HasClientCapability(const TString& capability) const override {
        return FindPtr(Ctx_->GetPeerMetaValues(NYdb::YDB_CLIENT_CAPABILITIES), capability);
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return ExtractDatabaseName(Ctx_->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER));
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        auto& s = Ctx_->GetAuthState();
        s.State = state;
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        return Ctx_->GetAuthState();
    }

    void ReplyWithRpcStatus(grpc::StatusCode code, const TString& reason, const TString& details) override {
        Ctx_->ReplyError(code, reason, details);
    }

    void ReplyUnauthenticated(const TString& in) override {
        Ctx_->ReplyUnauthenticated(MakeAuthError(in, IssueManager));
    }

    void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        InternalToken_ = token;
    }

    void AddServerHint(const TString& hint) override {
        Ctx_->AddTrailingMetadata(NYdb::YDB_SERVER_HINTS, hint);
    }

    void SetRuHeader(ui64 ru) override {
        Ru = ru;
        Ctx_->AddTrailingMetadata(NYdb::YDB_CONSUMED_UNITS_HEADER, IntToString<10>(ru));
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return InternalToken_;
    }

    const TString& GetSerializedToken() const override {
        if (InternalToken_) {
            return InternalToken_->GetSerializedToken();
        }

        return EmptySerializedTokenMessage_;
    }

    const TMaybe<TString> GetPeerMetaValues(const TString& key) const override {
        return ToMaybe(Ctx_->GetPeerMetaValues(key));
    }

    TVector<TStringBuf> FindClientCert() const override {
        return Ctx_->FindClientCert();
    }

    TVector<TStringBuf> FindClientCertPropertyValues() const override {
        return Ctx_->FindClientCert();
    }

    void SetDiskQuotaExceeded(bool disk) override {
        if (!QuotaExceeded) {
            QuotaExceeded = google::protobuf::Arena::CreateMessage<Ydb::QuotaExceeded>(GetArena());
        }
        QuotaExceeded->set_disk(disk);
    }

    bool GetDiskQuotaExceeded() const override {
        return QuotaExceeded ? QuotaExceeded->disk() : false;
    }

    bool Validate(TString&) override {
        return true;
    }

    void SetCounters(IGRpcProxyCounters::TPtr counters) override {
        Counters = counters;
    }

    IGRpcProxyCounters::TPtr GetCounters() const override {
        return Counters;
    }

    void UseDatabase(const TString& database) override {
        Ctx_->UseDatabase(database);
    }

    void ReplyUnavaliable() override {
        TResponse* resp = CreateResponseMessage();
        TCommonResponseFiller<TResp, TDerived::IsOp>::Fill(*resp, IssueManager.GetIssues(), CostInfo, Ydb::StatusIds::UNAVAILABLE);
        FinishRequest();
        Reply(resp, Ydb::StatusIds::UNAVAILABLE);
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        TResponse* resp = CreateResponseMessage();
        TCommonResponseFiller<TResponse, TDerived::IsOp>::Fill(*resp, IssueManager.GetIssues(), CostInfo, status);
        FinishRequest();
        Reply(resp, status);
    }

    TString GetPeerName() const override {
        return Ctx_->GetPeer();
    }

    bool SslServer() const {
        return Ctx_->SslServer();
    }

    template <typename T>
    static const TRequest* GetProtoRequest(const T& req) {
        auto request = dynamic_cast<const TRequest*>(req->GetRequest());
        Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper");
        return request;
    }

    template <typename T>
    static TRequest* GetProtoRequestMut(const T& req) {
        auto request = dynamic_cast<TRequest*>(req->GetRequestMut());
        Y_ABORT_UNLESS(request != nullptr, "Wrong using of TGRpcRequestWrapper");
        return request;
    }

    const TRequest* GetProtoRequest() const {
        return GetProtoRequest(this);
    }

    TMaybe<TString> GetTraceId() const override {
        return GetPeerMetaValues(NYdb::YDB_TRACE_ID_HEADER);
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return Span_.GetTraceId();
    }

    const TMaybe<TString> GetSdkBuildInfo() const {
        return GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    }

    TInstant GetDeadline() const override {
        return Ctx_->Deadline();
    }

    const TMaybe<TString> GetRequestType() const override {
        return GetPeerMetaValues(NYdb::YDB_REQUEST_TYPE_HEADER);
    }

    void SendSerializedResult(TString&& in, Ydb::StatusIds::StatusCode status, IRequestCtx::EStreamCtrl flag = IRequestCtx::EStreamCtrl::CONT) override {
        // res->data() pointer is used inside grpc code.
        // So this object should be destroyed during grpc_slice destroying routine
        auto res = new TString;
        res->swap(in);

        static auto freeResult = [](void* p) -> void {
            TString* toDelete = reinterpret_cast<TString*>(p);
            delete toDelete;
        };

        grpc_slice slice = grpc_slice_new_with_user_data(
                    (void*)(res->data()), res->size(), freeResult, res);
        grpc::Slice sl = grpc::Slice(slice, grpc::Slice::STEAL_REF);
        auto data = grpc::ByteBuffer(&sl, 1);
        Ctx_->Reply(&data, status, flag);
    }

    void SetCostInfo(float consumed_units) override {
        CostInfo = google::protobuf::Arena::CreateMessage<Ydb::CostInfo>(GetArena());
        CostInfo->set_consumed_units(consumed_units);
    }

    const TString& GetRequestName() const override {
        return TRequest::descriptor()->name();
    }

    google::protobuf::Arena* GetArena() override {
        return Ctx_->GetArena();
    }

    //! Allocate Result message using protobuf arena allocator
    //! The memory will be freed automaticaly after destroying
    //! corresponding request.
    //! Do not call delete for objects allocated here!
    template <typename TResult, typename T>
    static TResult* AllocateResult(T& ctx) {
        return google::protobuf::Arena::CreateMessage<TResult>(ctx->GetArena());
    }

    void SetStreamingNotify(NYdbGrpc::IRequestContextBase::TOnNextReply&& cb) override {
        Ctx_->SetNextReplyCallback(std::move(cb));
    }

    void SetFinishAction(std::function<void()>&& cb) override {
        auto shutdown = FinishWrapper(std::move(cb));
        Ctx_->GetFinishFuture().Subscribe(std::move(shutdown));
    }

    void SetCustomFinishWrapper(std::function<TFinishWrapper(std::function<void()>&&)> wrapper) {
        FinishWrapper = wrapper;
    }

    bool IsClientLost() const override {
        return Ctx_->IsClientLost();
    }

    void FinishStream(ui32 status) override {
        // End Of Request for streaming requests
        AuditLogRequestEnd(status);
        Ctx_->FinishStreamingOk();
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        IssueManager.RaiseIssue(issue);
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        IssueManager.RaiseIssues(issues);
    }

    const google::protobuf::Message* GetRequest() const override {
        return Ctx_->GetRequest();
    }

    google::protobuf::Message* GetRequestMut() override {
        return Ctx_->GetRequestMut();
    }

    void SetRespHook(TRespHook&& hook) override {
        RespHook = std::move(hook);
    }

    void SetRlPath(TMaybe<NRpcService::TRlPath>&& path) override {
        RlPath = std::move(path);
    }

    TMaybe<NRpcService::TRlPath> GetRlPath() const override {
        return RlPath;
    }

    void Pass(const IFacilityProvider&) override {
        Y_ABORT("unimplemented");
    }

    void SetAuditLogHook(TAuditLogHook&& hook) override {
        AuditLogHook = std::move(hook);
    }

    // IRequestCtx
    //
    void FinishRequest() override {
        RequestFinished = true;
    }

    // IRequestCtxBase
    //
    void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
        AuditLogParts.emplace_back(name, value);
    }
    const TAuditLogParts& GetAuditLogParts() const override {
        return AuditLogParts;
    }

    void StartTracing(NWilson::TSpan&& span) override {
        Span_ = std::move(span);
    }

    void FinishSpan() override {
        Span_.End();
    }

    bool* IsTracingDecided() override {
        return &IsTracingDecided_;
    }

    void ReplyGrpcError(grpc::StatusCode code, const TString& msg, const TString& details = "") {
        Ctx_->ReplyError(code, msg, details);
    }

private:
    void Reply(NProtoBuf::Message *resp, ui32 status) override {
        // End Of Request for non streaming requests
        if (RequestFinished) {
            AuditLogRequestEnd(status);
        }
        if (RespHook) {
            TRespHook hook = std::move(RespHook);
            return hook(MakeIntrusive<TRespHookCtx>(Ctx_, resp, GetRequestName(), Ru, status));
        }
        return Ctx_->Reply(resp, status);
    }

    void AuditLogRequestEnd(ui32 status) {
        if (AuditLogHook) {
            AuditLogHook(status, GetAuditLogParts());
            // Drop hook to avoid double logging in case when operation implemention
            // invokes both FinishRequest() (indirectly) and FinishStream()
            AuditLogHook = nullptr;
        }
    }

    TResponse* CreateResponseMessage() {
        return google::protobuf::Arena::CreateMessage<TResponse>(Ctx_->GetArena());
    }

    static TFinishWrapper GetStdFinishWrapper(std::function<void()>&& cb) {
        return [cb = std::move(cb)](const NYdbGrpc::IRequestContextBase::TAsyncFinishResult& future) mutable {
            Y_ASSERT(future.HasValue());
            if (future.GetValue() == NYdbGrpc::IRequestContextBase::EFinishStatus::CANCEL) {
                cb();
            }
        };
    }

protected:
    NWilson::TSpan Span_;
private:
    TIntrusivePtr<NYdbGrpc::IRequestContextBase> Ctx_;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken_;
    inline static const TString EmptySerializedTokenMessage_;
    NYql::TIssueManager IssueManager;
    Ydb::CostInfo* CostInfo = nullptr;
    Ydb::QuotaExceeded* QuotaExceeded = nullptr;
    ui64 Ru = 0;
    TRespHook RespHook;
    TMaybe<NRpcService::TRlPath> RlPath;
    IGRpcProxyCounters::TPtr Counters;
    std::function<TFinishWrapper(std::function<void()>&&)> FinishWrapper = &GetStdFinishWrapper;

    TAuditLogParts AuditLogParts;
    TAuditLogHook AuditLogHook;
    bool RequestFinished = false;
    bool IsTracingDecided_ = false;
};

template <ui32 TRpcId, typename TReq, typename TResp, bool IsOperation, typename TDerived>
class TGRpcRequestValidationWrapperImpl : public TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation, TDerived> {
public:
    static IActor* CreateRpcActor(typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type* msg);

    TGRpcRequestValidationWrapperImpl(NYdbGrpc::IRequestContextBase* ctx)
        : TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation, TDerived>(ctx)
    { }

    bool Validate(TString& error) override {
        return this->GetProtoRequest()->validate(error);
    }
};

// SFINAE
// Check protobuf has validate feature
template <typename TProto>
struct TProtoHasValidate {
private:
    static int Detect(...);
    // validate function has prototype: bool validate(TProtoStringType&) const
    static TProtoStringType Dummy_;
    template <typename U>
    static decltype(std::declval<U>().validate(Dummy_)) Detect(const U&);

public:
    static constexpr bool Value = std::is_same<bool, decltype(Detect(std::declval<TProto>()))>::value;
};

class IFacilityProvider;

template <typename TReq, typename TResp, bool IsOperation>
class TGrpcRequestCall
    : public std::conditional_t<TProtoHasValidate<TReq>::Value,
        TGRpcRequestValidationWrapperImpl<
            TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, IsOperation, TGrpcRequestCall<TReq, TResp, IsOperation>>,
        TGRpcRequestWrapperImpl<
            TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, IsOperation, TGrpcRequestCall<TReq, TResp, IsOperation>>>
{
    using TRequestIface = typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type;

public:
    static IActor* CreateRpcActor(TRequestIface* msg);
    static constexpr bool IsOp = IsOperation;

    using TBase = std::conditional_t<TProtoHasValidate<TReq>::Value,
        TGRpcRequestValidationWrapperImpl<
            TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, IsOperation, TGrpcRequestCall<TReq, TResp, IsOperation>>,
        TGRpcRequestWrapperImpl<
            TRpcServices::EvGrpcRuntimeRequest, TReq, TResp, IsOperation, TGrpcRequestCall<TReq, TResp, IsOperation>>>;

    template <typename TCallback>
    TGrpcRequestCall(NYdbGrpc::IRequestContextBase* ctx, TCallback&& cb, TRequestAuxSettings auxSettings = {})
        : TBase(ctx)
        , PassMethod(std::forward<TCallback>(cb))
        , AuxSettings(std::move(auxSettings))
    { }

    void Pass(const IFacilityProvider& facility) override {
        try {
            PassMethod(std::move(std::unique_ptr<TRequestIface>(this)), facility);
        } catch (const std::exception& ex) {
            this->RaiseIssue(NYql::TIssue{TStringBuilder() << "unexpected exception: " << ex.what()});
            this->ReplyWithYdbStatus(Ydb::StatusIds::INTERNAL_ERROR);
        }
    }

    TRateLimiterMode GetRlMode() const override {
        return AuxSettings.RlMode;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult& schemeData,
        ICheckerIface* iface) override
    {
        if (!AuxSettings.CustomAttributeProcessor) {
            return false;
        } else {
            AuxSettings.CustomAttributeProcessor(schemeData, iface);
            return true;
        }
    }

    NJaegerTracing::TRequestDiscriminator GetRequestDiscriminator() const override {
        return {
            .RequestType = AuxSettings.RequestType,
            .Database = TBase::GetDatabaseName(),
        };
    }

    // IRequestCtxBaseMtSafe
    //
    bool IsAuditable() const override {
        return (AuxSettings.AuditMode == TAuditMode::Auditable) && !this->IsInternalCall();
    }

private:
    std::function<void(std::unique_ptr<TRequestIface>, const IFacilityProvider&)> PassMethod;
    const TRequestAuxSettings AuxSettings;
};

template <typename TReq, typename TResp>
using TGrpcRequestOperationCall = TGrpcRequestCall<TReq, TResp, true>;

template <typename TReq, typename TResp>
using TGrpcRequestNoOperationCall = TGrpcRequestCall<TReq, TResp, false>;

template <ui32 TRpcId, typename TReq, typename TResp, bool IsOperation, TRateLimiterMode RlMode = TRateLimiterMode::Off>
class TGRpcRequestWrapper
    : public TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation,
        TGRpcRequestWrapper<TRpcId, TReq, TResp, IsOperation, RlMode>>
{
public:
    static IActor* CreateRpcActor(typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type* msg);
    static constexpr bool IsOp = IsOperation;
    static constexpr TRateLimiterMode RateLimitMode = RlMode;

    TGRpcRequestWrapper(NYdbGrpc::IRequestContextBase* ctx)
        : TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation,
            TGRpcRequestWrapper<TRpcId, TReq, TResp, IsOperation, RlMode>>(ctx)
    { }

    TRateLimiterMode GetRlMode() const override {
        return RateLimitMode;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult&, ICheckerIface*) override {
        return false;
    }
};

template <ui32 TRpcId, typename TReq, typename TResp, bool IsOperation = true, TRateLimiterMode RlMode = TRateLimiterMode::Off>
class TGRpcRequestWrapperNoAuth
    : public TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation,
        TGRpcRequestWrapperNoAuth<TRpcId, TReq, TResp, IsOperation, RlMode>>
{
public:
    static IActor* CreateRpcActor(typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type* msg);
    static constexpr bool IsOp = IsOperation;
    static constexpr TRateLimiterMode RateLimitMode = RlMode;

    TGRpcRequestWrapperNoAuth(NYdbGrpc::IRequestContextBase* ctx)
        : TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation,
            TGRpcRequestWrapperNoAuth<TRpcId, TReq, TResp, IsOperation, RlMode>>(ctx)
    { }

    TRateLimiterMode GetRlMode() const override {
        return RateLimitMode;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult&, ICheckerIface*) override {
        return false;
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        static NYdbGrpc::TAuthState noAuthState(false);
        return noAuthState;
    }
};

template <ui32 TRpcId, typename TReq, typename TResp, bool IsOperation, TRateLimiterMode RlMode = TRateLimiterMode::Off>
class TGRpcRequestValidationWrapper
    : public TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation,
        TGRpcRequestValidationWrapper<TRpcId, TReq, TResp, IsOperation, RlMode>>
{
public:
    static IActor* CreateRpcActor(typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type* msg);
    static constexpr bool IsOp = IsOperation;
    static constexpr TRateLimiterMode RateLimitMode = RlMode;

    TGRpcRequestValidationWrapper(NYdbGrpc::IRequestContextBase* ctx, bool rlAllowed = true)
        : TGRpcRequestWrapperImpl<TRpcId, TReq, TResp, IsOperation,
            TGRpcRequestValidationWrapper<TRpcId, TReq, TResp, IsOperation, RlMode>>(ctx)
        , RlAllowed(rlAllowed)
    { }

    TRateLimiterMode GetRlMode() const override {
        return RlAllowed ? RateLimitMode : TRateLimiterMode::Off;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult&, ICheckerIface*) override {
        return false;
    }

    bool Validate(TString& error) override {
        return this->GetProtoRequest()->validate(error);
    }

private:
    bool RlAllowed;
};

class TEvRequestAuthAndCheckResult : public TEventLocal<TEvRequestAuthAndCheckResult, TRpcServices::EvRequestAuthAndCheckResult> {
public:
    TEvRequestAuthAndCheckResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
        : Status(status)
        , Issues(issues)
    {}

    TEvRequestAuthAndCheckResult(Ydb::StatusIds::StatusCode status, const NYql::TIssue& issue)
        : Status(status)
    {
        Issues.AddIssue(issue);
    }

    TEvRequestAuthAndCheckResult(Ydb::StatusIds::StatusCode status, const TString& error)
        : Status(status)
    {
        Issues.AddIssue(error);
    }

    TEvRequestAuthAndCheckResult(const TString& database, const TMaybe<TString>& ydbToken, const TIntrusiveConstPtr<NACLib::TUserToken>& userToken)
        : Database(database)
        , YdbToken(ydbToken)
        , UserToken(userToken)
    {}

    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    NYql::TIssues Issues;
    TString Database;
    TMaybe<TString> YdbToken;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
};

class TEvRequestAuthAndCheck
    : public IRequestProxyCtx
    , public TEventLocal<TEvRequestAuthAndCheck, TRpcServices::EvRequestAuthAndCheck> {
public:
    TEvRequestAuthAndCheck(const TString& database, const TMaybe<TString>& ydbToken, NActors::TActorId sender)
        : Database(database)
        , YdbToken(ydbToken)
        , Sender(sender)
        , AuthState(true)
    {}

    // IRequestProxyCtx
    const TMaybe<TString> GetYdbToken() const override {
        return YdbToken;
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        AuthState.State = state;
    }

    void SetInternalToken(const TIntrusiveConstPtr<NACLib::TUserToken>& token) override {
        UserToken = token;
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        return AuthState;
    }

    void ReplyUnauthenticated(const TString& msg = "") override {
        if (msg) {
            IssueManager.RaiseIssue(NYql::TIssue{msg});
        }
        ReplyWithYdbStatus(Ydb::StatusIds::UNAUTHORIZED);
    }

    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode status) override {
        const NActors::TActorContext& ctx = NActors::TActivationContext::AsActorContext();
        if (status == Ydb::StatusIds::SUCCESS) {
            ctx.Send(Sender,
                new TEvRequestAuthAndCheckResult(
                    Database,
                    YdbToken,
                    UserToken
                )
            );
        } else {
            ctx.Send(Sender,
                new TEvRequestAuthAndCheckResult(
                    status,
                    IssueManager.GetIssues()
                )
            );
        }
    }

    void ReplyWithRpcStatus(grpc::StatusCode code, const TString& reason, const TString& details) override {
        Y_UNUSED(code);
        if (reason) {
            IssueManager.RaiseIssue(NYql::TIssue{reason});
        }
        if (details) {
            IssueManager.RaiseIssue(NYql::TIssue{details});
        }
        ReplyWithYdbStatus(Ydb::StatusIds::GENERIC_ERROR);
    }

    void ReplyUnavaliable() override {
        ReplyWithYdbStatus(Ydb::StatusIds::UNAVAILABLE);
    }

    void RaiseIssue(const NYql::TIssue& issue) override {
        IssueManager.RaiseIssue(issue);
    }

    void RaiseIssues(const NYql::TIssues& issues) override {
        IssueManager.RaiseIssues(issues);
    }

    TVector<TStringBuf> FindClientCertPropertyValues() const override {
        return {};
    }

    void StartTracing(NWilson::TSpan&& span) override {
        Span = std::move(span);
    }
    void FinishSpan() override {
        Span.End();
    }

    bool* IsTracingDecided() override {
        return nullptr;
    }

    bool Validate(TString& /*error*/) override {
        return true;
    }

    void SetCounters(IGRpcProxyCounters::TPtr counters) override {
        Counters = std::move(counters);
    }

    IGRpcProxyCounters::TPtr GetCounters() const override {
        return Counters;
    }

    bool HasClientCapability(const TString&) const override {
        return false;
    }

    void UseDatabase(const TString& database) override {
        Database = database;
    }

    void SetRespHook(TRespHook&& /*hook*/) override {
    }

    void SetRlPath(TMaybe<NRpcService::TRlPath>&& path) override {
        RlPath = std::move(path);
    }

    TRateLimiterMode GetRlMode() const override {
        return TRateLimiterMode::Rps;
    }

    bool TryCustomAttributeProcess(const TSchemeBoardEvents::TDescribeSchemeResult& /*schemeData*/, ICheckerIface* /*iface*/) override {
        return false;
    }

    void Pass(const IFacilityProvider& /*facility*/) override {
        ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
    }

    void SetAuditLogHook(TAuditLogHook&& /*hook*/) override {
    }

    void SetDiskQuotaExceeded(bool /*disk*/) override {
    }

    void AddAuditLogPart(const TStringBuf& name, const TString& value) override {
        AuditLogParts.emplace_back(name, value);
    }

    const TAuditLogParts& GetAuditLogParts() const override {
        return AuditLogParts;
    }

    TMaybe<TString> GetTraceId() const override {
        return {};
    }

    NWilson::TTraceId GetWilsonTraceId() const override {
        return Span.GetTraceId();
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return Database ? TMaybe<TString>(Database) : Nothing();
    }

    const TIntrusiveConstPtr<NACLib::TUserToken>& GetInternalToken() const override {
        return UserToken;
    }

    const TString& GetSerializedToken() const override {
        if (UserToken) {
            return UserToken->GetSerializedToken();
        }

        return EmptySerializedTokenMessage;
    }

    bool IsClientLost() const override {
        return false;
    }

    const TMaybe<TString> GetPeerMetaValues(const TString&) const override {
        return {};
    }

    TString GetPeerName() const override {
        return {};
    }

    const TString& GetRequestName() const override {
        static TString str = "request auth and check internal request";
        return str;
    }

    TMaybe<NRpcService::TRlPath> GetRlPath() const override {
        return RlPath;
    }

    TInstant GetDeadline() const override {
        return deadline;
    }

    bool GetDiskQuotaExceeded() const override {
        return false;
    }

    TMaybe<TString> GetSdkBuildInfo() const {
        return {};
    }

    TMaybe<TString> GetGrpcUserAgent() const {
        return {};
    }

    TVector<TStringBuf> FindClientCert() const override {
        return {};
    }

    TString Database;
    TMaybe<TString> YdbToken;
    NActors::TActorId Sender;
    NYdbGrpc::TAuthState AuthState;
    NWilson::TSpan Span;
    IGRpcProxyCounters::TPtr Counters;
    TMaybe<NRpcService::TRlPath> RlPath;
    TAuditLogParts AuditLogParts;
    NYql::TIssueManager IssueManager;
    TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TInstant deadline = TInstant::Now() + TDuration::Seconds(10);

    inline static const TString EmptySerializedTokenMessage;
};

} // namespace NGRpcService
} // namespace NKikimr
