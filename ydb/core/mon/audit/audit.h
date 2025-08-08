#pragma once

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>

namespace NMonitoring::NAudit {

using TAuditParts = TVector<std::pair<TString, TString>>;

class TAuditCtx {
    enum ERequestStatus {
        Success,
        Process,
        Error,
    };

    enum EState : ui8 {
        Init = 0,
        PendingExecution = 1,
        Executing = 2,
        Completed = 3
    };

public:
    void InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev);
    void AddAuditLogParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);
    void LogAudit(ERequestStatus status, const TString& reason);
    void LogOnExecute();
    void LogOnResult(const NHttp::THttpOutgoingResponsePtr& response);

private:
    static ERequestStatus GetStatus(const NHttp::THttpOutgoingResponsePtr response);
    static TString ToString(const ERequestStatus value);
    static TString GetReason(const NHttp::THttpOutgoingResponsePtr& response);
    void AddAuditLogPart(TStringBuf name, const TString& value);
    bool AuditableRequest(const TString& method, const TString& url, const TCgiParameters& cgiParams);

    TAuditParts Parts;
    bool Auditable = false;
    EState State = EState::Init;

    NACLibProto::ESubjectType SubjectType = NACLibProto::SUBJECT_TYPE_ANONYMOUS;
    TString Subject;
    TString SanitizedToken;
};

bool HttpAuditEnabled(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);
bool HttpAuditEnabled(TString serializedToken);

}
