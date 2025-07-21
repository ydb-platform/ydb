#pragma once

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>

namespace NActors {

using TAuditParts = TVector<std::pair<TString, TString>>;

bool NeedAudit(const TString& method, const TString& url, const TCgiParameters& params,
               TStringBuf TStringBuf = {});

void AddAuditParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TAuditParts& parts);

void AddErrorStatusAuditParts(const TString& status, TAuditParts& parts);

void AuditRequest(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev,
                  const NHttp::THttpOutgoingResponsePtr response,
                  const TAuditParts& parts = {});

}
