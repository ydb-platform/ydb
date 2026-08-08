#pragma once

#include <ydb/core/protos/config.pb.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>

namespace NMonitoring::NAudit {

TString ExtractRemoteAddress(const NHttp::THttpIncomingRequest* request);

using TAuditParts = TVector<std::pair<TString, TString>>;

enum ERequestStatus {
    Success,
    Process,
    Error,
};

class TAuditCtx {
public:
    void InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, bool needAudit = true);
    void AddAuditLogParts(const TAuditParts& parts); // TODO: pass request context instead of audit log parts
    void LogOnReceived();
    void LogOnCompleted(const NHttp::THttpOutgoingResponsePtr& response);
    void SetSubjectType(NACLibProto::ESubjectType subjectType);
    static bool AuditEnabled(NKikimrConfig::TAuditConfig::TLogClassConfig::ELogPhase logPhase, NACLibProto::ESubjectType subjectType);
    static bool AuditableRequest(const NHttp::THttpIncomingRequestPtr& request);

private:
    void AddAuditLogPart(TStringBuf name, const TString& value);
    void LogAudit(ERequestStatus status, const TString& reason, NKikimrConfig::TAuditConfig::TLogClassConfig::ELogPhase logPhase);

    TAuditParts Parts;
    bool Auditable = false;
    NACLibProto::ESubjectType SubjectType = NACLibProto::SUBJECT_TYPE_ANONYMOUS;
    TString Subject;
    TString SanitizedToken;
};

}
