#pragma once

#include <ydb/core/base/auth.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>

namespace NActors {

using TAuditParts = TVector<std::pair<TString, TString>>;

bool IsAuditEnabled(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev);

void AddUserTokenAuditParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TAuditParts& parts);

void AddErrorStatusAuditParts(const TString& status, TAuditParts& parts);

void AuditRequest(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev,
                  const TAuditParts& parts = {});

}
