#include "remote_embedder.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/query_service_config.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/regex/pcre/regexp.h>
#include <library/cpp/uri/uri.h>

#include <util/string/ascii.h>

namespace NKnn {

void ValidateRemoteEmbedderDestination(TStringBuf secretName, TStringBuf host, ui16 port, bool useTls) {
    if (!NKikimr::HasAppData()) {
        // Standalone C++ hosts supply their own credentials and have no database secret resolver.
        Y_ENSURE(!secretName.StartsWith('/'), "RemoteEmbedder: database secret policy is unavailable");
        return;
    }
    const auto* appData = NKikimr::AppData();
    const auto config = NKikimr::LoadQueryServiceConfig(*appData);
    const auto& settings = config ? *config : appData->QueryServiceConfig;
    Y_ENSURE(useTls, "RemoteEmbedder: HTTPS is required for database secrets");

    const auto& origins = settings.GetRemoteEmbedderSecretOrigins();
    const auto it = origins.find(TString(secretName));
    Y_ENSURE(secretName.StartsWith('/') && appData->FeatureFlags.GetEnableSchemaSecrets() && it != origins.end(),
        "RemoteEmbedder: no approved HTTPS origin for the database secret");
    NUri::TUri origin;
    Y_ENSURE(origin.ParseAbs(it->second, NUri::TFeature::FeaturesDefaultOrSchemeKnown) == NUri::TState::ParsedOK
        && origin.GetScheme() == NUri::TScheme::SchemeHTTPS
        && !(origin.GetFieldMask() & (NUri::TField::FlagAuth | NUri::TField::FlagQuery | NUri::TField::FlagFrag))
        && (origin.GetField(NUri::TField::FieldPath).empty() || origin.GetField(NUri::TField::FieldPath) == "/"),
        "RemoteEmbedder: configured secret origin must be an HTTPS origin");
    auto approvedHost = origin.GetHost();
    if (approvedHost.StartsWith('[') && approvedHost.EndsWith(']')) {
        approvedHost = approvedHost.SubStr(1, approvedHost.size() - 2);
    }
    Y_ENSURE(AsciiEqualsIgnoreCase(approvedHost, host) && origin.GetPort() == port,
        "RemoteEmbedder: destination does not match the approved HTTPS origin for the database secret");

    const auto& patterns = settings.GetHostnamePatterns();
    if (patterns.empty()) {
        return;
    }
    const TString hostname(host);
    for (const auto& pattern : patterns) {
        if (TRegExMatch(pattern).Match(hostname.c_str())) {
            return;
        }
    }
    ythrow yexception() << "RemoteEmbedder: It is not allowed to access hostname '" << hostname << "'";
}

} // namespace NKnn
