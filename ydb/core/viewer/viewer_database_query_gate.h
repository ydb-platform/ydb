#pragma once

#include <library/cpp/monlib/service/service.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/http/http.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NViewer {

bool ViewerHttpPathRequiresExplicitDatabaseQuery(TStringBuf pathWithoutQuery);

bool MonHttpRequestHasExplicitDatabaseSelector(const NMonitoring::IMonHttpRequest& request);

bool HttpRequestHasExplicitDatabaseSelector(const NHttp::THttpRequest& request);

enum class EViewerJsonDatabaseQueryGateResult {
    Allow,
    /// Token is allowed only via `database_allowed_sids` must pass explicit DB scope.
    ForbiddenMissingDatabaseQueryForDatabaseSid,
};

/// For paths in the allowlist: missing `database`/`tenant` yields 403 only for `database_allowed_sids`;
EViewerJsonDatabaseQueryGateResult EvaluateViewerJsonDatabaseQueryGate(
    const NKikimrConfig::TDomainsConfig& domainsConfig,
    TStringBuf pathWithoutQuery,
    const TString& userTokenSerialized,
    bool hasExplicitDatabaseInQuery);

} // namespace NKikimr::NViewer
