#include "external_service.h"

#include <ydb/core/fq/libs/config/yq_issue.h>
#include <library/cpp/uri/uri.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <algorithm>

namespace NFq {

NYql::TIssues ValidateExternalService(const FederatedQuery::ExternalService& service, bool disableCurrentIam) {
    NYql::TIssues issues;
    auto error = [&](const char* message) { issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, message)); };
    const bool http = service.protocol() == FederatedQuery::ExternalService::HTTP;
    const bool grpc = service.protocol() == FederatedQuery::ExternalService::GRPC;
    if (!http && !grpc)
        error("external_service.protocol must be HTTP or GRPC");

    const TString endpoint = service.endpoint();
    if (endpoint.empty() || endpoint.size() > 4096 || endpoint.find_first_of("\r\n\t ") != TString::npos || endpoint.Contains('\0')) {
        error("Invalid external_service.endpoint");
    } else {
        NUri::TUri uri;
        const TString url = http ? endpoint : "http://" + endpoint;
        const auto status = uri.ParseUri(url, NUri::TFeature::FeatureSchemeKnown);
        if (status != NUri::TState::ParsedOK || uri.GetField(NUri::TField::FieldHost).empty() ||
            uri.FldIsSet(NUri::TField::FieldUser) || uri.FldIsSet(NUri::TField::FieldPass) || uri.FldIsSet(NUri::TField::FieldFrag)) {
            error("Invalid external_service.endpoint");
        }
        if (http && !endpoint.StartsWith(service.insecure() ? "http://" : "https://"))
            error("external_service.endpoint scheme must match insecure flag");
        if (grpc) {
            ui32 port = 0;
            if (endpoint.find_first_of("/?#@") != TString::npos || !TryFromString(uri.GetField(NUri::TField::FieldPort), port) ||
                !port || port > 65535)
                error("external_service gRPC endpoint must be host:port");
        }
    }
    if (http && !service.method().empty() && service.method() != "POST" && service.method() != "GET" &&
        service.method() != "PUT" && service.method() != "DELETE")
        error("Unsupported external_service HTTP method");
    if (grpc && (service.method().size() < 4 || service.method()[0] != '/' ||
                 service.method().find('/', 1) == TString::npos || service.method().back() == '/' ||
                 service.method().find_first_of("\r\n\t ") != TString::npos || service.method().find('\0') != TString::npos))
        error("external_service gRPC method must be /package.Service/Method");
    if (service.method().size() > 1024)
        error("external_service.method exceeds limit");
    if (service.ca_certificate().size() > 65536 || (service.insecure() && !service.ca_certificate().empty()))
        error("external_service.ca_certificate requires TLS and must not exceed 65536 bytes");

    const auto& auth = service.auth();
    if (!service.has_auth() || auth.identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET)
        error("external_service.auth is not specified");
    if (auth.has_current_iam() && disableCurrentIam)
        error("current iam authorization is disabled");
    if (auth.has_token() && (auth.token().token().empty() || auth.token().token().size() > 1024 ||
                             !std::all_of(auth.token().token().begin(), auth.token().token().end(), [](unsigned char c) {
                                 return c > 32 && c < 127;
                             })))
        error("Invalid external_service auth token");
    if (auth.has_service_account())
        error("external_service service account authentication is not supported yet");

    size_t bytes = 0;
    for (const auto& [key, value] : service.headers()) {
        const TString lower = to_lower(TString(key));
        const bool validKey = !key.empty() && std::all_of(key.begin(), key.end(), [](char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
        });
        const bool validValue = std::all_of(value.begin(), value.end(), [grpc](unsigned char c) {
            return c != 0 && c != '\r' && c != '\n' && (!grpc || (c >= 32 && c < 127));
        });
        if (!validKey || (grpc && key != lower) || !validValue ||
            lower == "authorization" || lower == "host" || lower == "content-length" || lower == "transfer-encoding" ||
            lower == "connection" || lower == "te" || lower == "trailer" || (grpc && lower.EndsWith("-bin")))
            error("Invalid or reserved external_service header");
        bytes += key.size() + value.size();
    }
    if (service.headers_size() > 32 || bytes > 16384)
        error("external_service headers exceed limit");
    return issues;
}

} // namespace NFq
