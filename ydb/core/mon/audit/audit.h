#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/grpc_services/base/base.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>

namespace NMonitoring::NAudit {

using TAuditParts = TVector<std::pair<TString, TString>>;

enum ERequestStatus {
    Success,
    Process,
    Error,
};

class TAuditCtx {
public:
    void InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev);
    void AddAuditLogParts(const NKikimr::NGRpcService::TEvRequestAuthAndCheckResult* result);
    void LogAudit(ERequestStatus status, const TString& reason, NKikimrConfig::TAuditConfig::TLogClassConfig::ELogPhase logPhase);
    void LogOnReceived();
    void LogOnCompleted(const NHttp::THttpOutgoingResponsePtr& response);
    static const NKikimrConfig::TAuditConfig::TLogClassConfig::ELogClass MONITORING_LOG_CLASS = NKikimrConfig::TAuditConfig::TLogClassConfig::ClusterAdmin;

private:
    void AddAuditLogPart(TStringBuf name, const TString& value);
    bool AuditableRequest(const NHttp::THttpIncomingRequestPtr& request);

    TAuditParts Parts;
    bool Auditable = false;
    NACLibProto::ESubjectType SubjectType = NACLibProto::SUBJECT_TYPE_ANONYMOUS;
};

}
