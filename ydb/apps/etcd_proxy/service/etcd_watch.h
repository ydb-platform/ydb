#pragma once

#include "events.h"

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr::NGRpcService {

template <ui32 TRpcId, typename TReq, typename TResp>
class TEtcdRequestStreamWrapper
    : public IRequestProxyCtx
    , public TEventLocal<TEtcdRequestStreamWrapper<TRpcId, TReq, TResp>, TRpcId>
{
private:
    void ReplyWithYdbStatus(Ydb::StatusIds::StatusCode) override {
        Ctx_->Attach(TActorId());
        TResponse resp;
        Ctx_->WriteAndFinish(std::move(resp), grpc::Status::OK);
    }
public:
    using TRequest = TReq;
    using TResponse = TResp;
    using IStreamCtx = NGRpcServer::IGRpcStreamingContext<TRequest, TResponse>;

    TEtcdRequestStreamWrapper(TIntrusivePtr<IStreamCtx> ctx, TRequestAuxSettings auxSettings = {})
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

    TRateLimiterMode GetRlMode() const override {
        return AuxSettings.RlMode;
    }

    bool TryCustomAttributeProcess(const NKikimrScheme::TEvDescribeSchemeResult& schemeData,
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
            .Database = GetDatabaseName(),
        };
    }

    bool IsAuditable() const override {
        return (AuxSettings.AuditMode == TAuditMode::Auditable) && !this->IsInternalCall();
    }

    const TMaybe<TString> GetYdbToken() const override {
        return ExtractYdbToken(Ctx_->GetPeerMetaValues(NYdb::YDB_AUTH_TICKET_HEADER));
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

    void ReplyUnauthenticated(const TString& in) override {
        Ctx_->Finish(grpc::Status(grpc::StatusCode::UNAUTHENTICATED, MakeAuthError(in, IssueManager_)));
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
        return ToMaybe(Ctx_->GetPeerMetaValues(key));
    }

    void SetDiskQuotaExceeded(bool) override {
    }

    void RefreshToken(const TString& token, const TActorContext& ctx, TActorId id, NWilson::TTraceId traceId = {});

    void SetRespHook(TRespHook&&) override {
        /* cannot add hook to bidirect streaming */
        Y_ABORT("Unimplemented");
    }

    void Pass(const IFacilityProvider&) override {
        Y_ABORT("unimplemented");
    }

    void SetAuditLogHook(TAuditLogHook&&) override {
        Y_ABORT("unimplemented for TEtcdRequestStreamWrapper");
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
        Y_ABORT("unimplemented for TEtcdRequestStreamWrapper");
    }
    const TAuditLogParts& GetAuditLogParts() const override {
        Y_ABORT("unimplemented for TEtcdRequestStreamWrapper");
    }

private:
    TIntrusivePtr<IStreamCtx> Ctx_;
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken_;
    inline static const TString EmptySerializedTokenMessage_;
    NYql::TIssueManager IssueManager_;
    TMaybe<NRpcService::TRlPath> RlPath_;
    IGRpcProxyCounters::TPtr Counters_;
    NWilson::TSpan Span_;
    bool IsTracingDecided_ = false;
    TULIDGenerator UlidGen;
    TMaybe<TString> TraceId;
    const TRequestAuxSettings AuxSettings;
};

using TEvWatchRequest = TEtcdRequestStreamWrapper<NEtcd::EvWatch, etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;

}

namespace NEtcd {

NActors::IActor* CreateEtcdWatchtower(TIntrusivePtr<NMonitoring::TDynamicCounters> counters);

}

