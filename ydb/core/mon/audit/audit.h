#pragma once

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>

namespace NMonitoring::NAudit {

using TAuditParts = TVector<std::pair<TString, TString>>;

class TAuditCtx {
    enum class ERequestStatus {
        Success,
        Process,
        Error,
    };

    enum class EState : ui8 {
        Init = 0,
        Executing,
        Completed,
    };

public:
    void Init(const NActors::NMon::TEvHttpInfo::TPtr& ev);
    void Init(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev);
    void SetUserToken(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);
    void SetUserToken(const NACLibProto::TUserToken& userToken);
    void LogOnExecute();
    void LogOnResult(const NHttp::THttpOutgoingResponsePtr& response);

private:
    static ERequestStatus GetStatus(const NHttp::THttpOutgoingResponsePtr response);
    static TString ToString(const ERequestStatus value);
    static TString GetReason(const NHttp::THttpOutgoingResponsePtr& response);
    void LogAudit(ERequestStatus status, const TString& reason);
    void AddAuditLogPart(TStringBuf name, const TString& value);
    bool AuditableRequest(const HTTP_METHOD method, const TString& url, const TCgiParameters& cgiParams);

    NACLibProto::ESubjectType SubjectType = NACLibProto::SUBJECT_TYPE_ANONYMOUS;
    TAuditParts Parts;
    EState State = EState::Init;
};

bool HttpAuditEnabled(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);
// bool HttpAuditEnabled(TString serializedToken);

}
