#pragma once

#include <ydb/core/base/auth.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>

#include <util/generic/string.h>

namespace NActors {

using TAuditParts = TVector<std::pair<TString, TString>>;

bool IsAuditEnabled(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev, TMon::EAuditPolicy auditPolicy);

void AddUserTokenAuditParts(const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TAuditParts& parts);

void AuditRequest(const NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev,
                  const TMon::EAuditPolicy auditPolicy,
                  const TAuditParts& parts = {});

}
