#pragma once

#include <ydb/core/base/events.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NEtcd {

struct TData {
    std::string Value;
    i64 Created = 0LL, Modified = 0LL, Version = 0LL, Lease = 0LL;
};

enum class EWatchKind : ui8 {
    Unsubscribe = 0,
    OnUpdates,
    OnDeletions,
    OnChanges = OnUpdates | OnDeletions
};

enum EEv : ui32 {
    EvBegin = 5000,
    EvQueryResult,
    EvQueryError,

    EvWatch,
    EvLeaseKeepAlive,

    EvSubscribe,
    EvChange,
    EvEnd
};

struct TEvQueryResult : public NActors::TEventLocal<TEvQueryResult, EvQueryResult> {
    TEvQueryResult(const NYdb::TResultSets& result): Results(result) {}

    const NYdb::TResultSets Results;
};

struct TEvQueryError : public NActors::TEventLocal<TEvQueryError, EvQueryError> {
    TEvQueryError(const NYdb::NIssue::TIssues& issues): Issues(issues) {}

    const NYdb::NIssue::TIssues Issues;
};

struct TEvSubscribe : public NActors::TEventLocal<TEvSubscribe, EvSubscribe> {
    const std::string Key, RangeEnd;
    const EWatchKind Kind;
    bool WithPrevious = false;

    TEvSubscribe(std::string_view key = {}, std::string_view rangeEnd = {}, EWatchKind kind = EWatchKind::Unsubscribe, bool withPrevious = false)
        : Key(key), RangeEnd(rangeEnd), Kind(kind), WithPrevious(withPrevious)
    {}
};

struct TEvChange : public NActors::TEventLocal<TEvChange, EvChange> {
    TEvChange(std::string&& key, TData&& oldData, TData&& newData = {})
        : Key(std::move(key)), OldData(std::move(oldData)), NewData(std::move(newData))
    {}

    TEvChange(const TEvChange& put)
        : Key(put.Key), OldData(put.OldData), NewData(put.NewData)
    {}

    const std::string Key;
    const TData OldData, NewData;
};

template <ui32 TRpcId, typename TReq, typename TResp>
class TEtcdRequestStreamWrapper
    : public NKikimr::NGRpcService::IRequestProxyCtx
    , public NActors::TEventLocal<TEtcdRequestStreamWrapper<TRpcId, TReq, TResp>, TRpcId>
{
private:
    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode) override {
        Ctx_->Attach({});
        TResponse resp;
        Ctx_->WriteAndFinish(std::move(resp), grpc::Status::OK);
    }
public:
    using TRequest = TReq;
    using TResponse = TResp;
    using IStreamCtx = NKikimr::NGRpcServer::IGRpcStreamingContext<TRequest, TResponse>;

    TEtcdRequestStreamWrapper(TIntrusivePtr<IStreamCtx> ctx, NKikimr::NGRpcService::TRequestAuxSettings auxSettings = {})
        : Ctx_(ctx)
        , TraceId(GetPeerMetaValues(NYdb::YDB_TRACE_ID_HEADER))
        , AuxSettings(std::move(auxSettings))
    {
        if (!TraceId) {
            TraceId = UlidGen.Next().ToString();
        }
    }

    bool IsClientLost() const override {
        // TODO: Implement for BiDirectional streaming
        return false;
    }

    NKikimr::NGRpcService::TRateLimiterMode GetRlMode() const override {
        return AuxSettings.RlMode;
    }

    bool TryCustomAttributeProcess(const NKikimrScheme::TEvDescribeSchemeResult& schemeData, NKikimr::NGRpcService::ICheckerIface* iface) override {
        if (!AuxSettings.CustomAttributeProcessor) {
            return false;
        } else {
            AuxSettings.CustomAttributeProcessor(schemeData, iface);
            return true;
        }
    }

    NKikimr::NJaegerTracing::TRequestDiscriminator GetRequestDiscriminator() const override {
        return {
            .RequestType = AuxSettings.RequestType,
            .Database = GetDatabaseName(),
        };
    }

    bool IsAuditable() const override {
        return (AuxSettings.AuditMode == NKikimr::NGRpcService::TAuditMode::Auditable) && !this->IsInternalCall();
    }

    const TMaybe<TString> GetYdbToken() const override {
        return NKikimr::NGRpcService::ExtractYdbToken(Ctx_->GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER));
    }

    const TMaybe<TString> GetDatabaseName() const override {
        return NKikimr::NGRpcService::ExtractDatabaseName(Ctx_->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER));
    }

    void UpdateAuthState(NYdbGrpc::TAuthState::EAuthState state) override {
        auto& s = Ctx_->GetAuthState();
        s.State = state;
    }

    const NYdbGrpc::TAuthState& GetAuthState() const override {
        return Ctx_->GetAuthState();
    }

    void ReplyUnauthenticated(const TString& in) override {
        Ctx_->Finish(grpc::Status(grpc::StatusCode::UNAUTHENTICATED, NKikimr::NGRpcService::MakeAuthError(in, IssueManager_)));
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

    void SetRlPath(TMaybe<NKikimr::NRpcService::TRlPath>&& path) override {
        RlPath_ = std::move(path);
    }

    TMaybe<NKikimr::NRpcService::TRlPath> GetRlPath() const override {
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

    void SetCounters(NKikimr::NGRpcService::IGRpcProxyCounters::TPtr counters) override {
        Counters_ = counters;
    }

    NKikimr::NGRpcService::IGRpcProxyCounters::TPtr GetCounters() const override {
        return Counters_;
    }

    void UseDatabase(const TString& database) override {
        Ctx_->UseDatabase(database);
    }

    TVector<TStringBuf> FindClientCertPropertyValues() const override {
        return {};
    }

    IStreamCtx* GetStreamCtx() const {
        return Ctx_.Get();
    }

    TIntrusivePtr<IStreamCtx> ReleaseStreamCtx() const {
        return Ctx_.Release();
    }

    const TString& GetRequestName() const override {
        return TRequest::descriptor()->name();
    }

    TMaybe<TString> GetTraceId() const override {
        return TraceId;
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
        return NKikimr::NGRpcService::ToMaybe(Ctx_->GetPeerMetaValues(key));
    }

    void SetDiskQuotaExceeded(bool) override {
    }

    void RefreshToken(const TString& token, const NActors::TActorContext& ctx, NActors::TActorId id, NWilson::TTraceId traceId = {});

    void SetRespHook(NKikimr::NGRpcService::TRespHook&&) override {
        Y_ABORT("Unimplemented!");
    }

    void Pass(const NKikimr::NGRpcService::IFacilityProvider&) override {
        Y_ABORT("Unimplemented!");
    }

    void SetAuditLogHook(NKikimr::NGRpcService::TAuditLogHook&&) override {
        Y_ABORT("Unimplemented!");
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
        Y_ABORT("Unimplemented!");
    }
    const NKikimr::NGRpcService::TAuditLogParts& GetAuditLogParts() const override {
        Y_ABORT("Unimplemented!");
    }
private:
    TIntrusivePtr<IStreamCtx> Ctx_;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken_;
    inline static const TString EmptySerializedTokenMessage_;
    NYql::TIssueManager IssueManager_;
    TMaybe<NKikimr::NRpcService::TRlPath> RlPath_;
    NKikimr::NGRpcService::IGRpcProxyCounters::TPtr Counters_;
    NWilson::TSpan Span_;
    bool IsTracingDecided_ = false;
    NKikimr::TULIDGenerator UlidGen;
    TMaybe<TString> TraceId;
    const NKikimr::NGRpcService::TRequestAuxSettings AuxSettings;
};

using TEvWatchRequest = TEtcdRequestStreamWrapper<NEtcd::EvWatch, etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;
using TEvLeaseKeepAliveRequest = TEtcdRequestStreamWrapper<NEtcd::EvLeaseKeepAlive, etcdserverpb::LeaseKeepAliveRequest, etcdserverpb::LeaseKeepAliveResponse>;

} // namespace NEtcd
