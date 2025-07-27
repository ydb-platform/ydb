#pragma once

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>

namespace NActors {

using TAuditParts = TVector<std::pair<TString, TString>>;

class TAuditCtx {
public:
    void InitAudit(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev);
    void AddAuditLogParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken);
    void FinishAudit(const NHttp::THttpOutgoingResponsePtr& response);

private:
    void AddAuditLogPart(TStringBuf name, const TString& value);
    bool CheckAuditConditions(const TString& method);

    TAuditParts Parts;
    bool Need = false;
};

}
