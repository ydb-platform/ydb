#include "external_service.h"

#include <ydb/core/fq/libs/config/yq_issue.h>

#include <library/cpp/uri/uri.h>
#include <library/cpp/json/json_reader.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>

#include <algorithm>

namespace NFq {

NYql::TIssues ValidateExternalService(const FederatedQuery::ExternalService& service, bool disableCurrentIam) {
    NYql::TIssues issues;
    auto error = [&](const char* message) {
        issues.AddIssue(MakeErrorIssue(TIssuesIds::BAD_REQUEST, message));
    };
    const bool http = service.protocol() == FederatedQuery::ExternalService::HTTP;
    const bool grpc = service.protocol() == FederatedQuery::ExternalService::GRPC;
    if (!http && !grpc) {
        error("external_service.protocol must be HTTP or GRPC");
    }

    const TString endpoint = service.endpoint();
    if (endpoint.empty() || endpoint.size() > 4096 || endpoint.find_first_of("\r\n\t ") != TString::npos || endpoint.Contains('\0')) {
        error("Invalid external_service.endpoint");
    } else {
        NUri::TUri uri;
        const TString url = http ? endpoint : "http://" + endpoint;
        const auto status = uri.ParseUri(url, NUri::TFeature::FeatureSchemeKnown);
        if (status != NUri::TState::ParsedOK || uri.GetField(NUri::TField::FieldHost).empty() || uri.FldIsSet(NUri::TField::FieldUser) ||
            uri.FldIsSet(NUri::TField::FieldPass) || uri.FldIsSet(NUri::TField::FieldFrag)) {
            error("Invalid external_service.endpoint");
        }
        if (http && !endpoint.StartsWith(service.insecure() ? "http://" : "https://")) {
            error("external_service.endpoint scheme must match insecure flag");
        }
        if (http && ((!uri.GetField(NUri::TField::FieldPath).empty() && uri.GetField(NUri::TField::FieldPath) != "/") ||
                        uri.FldIsSet(NUri::TField::FieldQuery))) {
            error("external_service HTTP endpoint must be an origin; WASM supplies the path");
        }
        if (grpc) {
            ui32 port = 0;
            if (endpoint.find_first_of("/?#@") != TString::npos || !TryFromString(uri.GetField(NUri::TField::FieldPort), port) || !port ||
                port > 65535) {
                error("external_service gRPC endpoint must be host:port");
            }
        }
    }
    if (service.ca_certificate().size() > 65536 || (service.insecure() && !service.ca_certificate().empty())) {
        error("external_service.ca_certificate requires TLS and must not exceed 65536 bytes");
    }

    const auto& auth = service.auth();
    if (int(service.has_auth()) + int(service.has_tvm_auth()) + int(service.has_wasm_auth()) != 1) {
        error("Exactly one of external_service.auth, external_service.tvm_auth and external_service.wasm_auth must be specified");
    }
    if (service.has_auth() && auth.identity_case() == FederatedQuery::IamAuth::IDENTITY_NOT_SET) {
        error("external_service.auth is not specified");
    }
    if (service.has_wasm_auth() && (service.wasm_auth().provider().empty() || service.wasm_auth().provider().size() > 128 ||
                                       service.wasm_auth().parameters().empty() || service.wasm_auth().parameters().size() > 65536)) {
        error("Invalid external_service.wasm_auth");
    }
    if (service.has_wasm_auth()) {
        NJson::TJsonValue params;
        if (!NJson::ReadJsonTree(service.wasm_auth().parameters(), &params) || !params.IsMap() || params.GetMap().size() > 16) {
            error("Invalid external_service.wasm_auth parameters");
        } else {
            for (const auto& [name, value] : params.GetMap()) {
                if (name.empty() || name.size() > 128 || !value.IsString() || value.GetString().size() > 4096) {
                    error("Invalid external_service.wasm_auth parameter");
                }
            }
        }
    }
    if (service.has_tvm_auth()) {
        const auto& tvm = service.tvm_auth();
        if (!tvm.source_id() || !tvm.destination_id()) {
            error("external_service.tvm_auth requires nonzero source_id and destination_id");
        }
        if (tvm.secret().empty() || tvm.secret().size() > 4096) {
            error("external_service.tvm_auth secret must contain 1 to 4096 bytes");
        }
    }
    if (auth.has_current_iam() && disableCurrentIam) {
        error("current iam authorization is disabled");
    }
    if (auth.has_token() && (auth.token().token().empty() || auth.token().token().size() > 1024 ||
                                !std::all_of(auth.token().token().begin(), auth.token().token().end(), [](unsigned char c) {
                                    return c > 32 && c < 127;
                                }))) {
        error("Invalid external_service auth token");
    }
    if (auth.has_service_account()) {
        error("external_service service account authentication is not supported yet");
    }

    size_t bytes = 0;
    for (const auto& [key, value] : service.headers()) {
        const TString lower = to_lower(TString(key));
        const bool validKey = !key.empty() && std::all_of(key.begin(), key.end(), [](char c) {
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.';
        });
        const bool validValue = std::all_of(value.begin(), value.end(), [grpc](unsigned char c) {
            return c != 0 && c != '\r' && c != '\n' && (!grpc || (c >= 32 && c < 127));
        });
        if (!validKey || (grpc && key != lower) || !validValue || lower == "authorization" || lower == "x-ya-service-ticket" ||
            lower == "x-ya-user-ticket" || lower == "host" || lower == "content-length" || lower == "transfer-encoding" ||
            lower == "connection" || lower == "te" || lower == "trailer" || (grpc && lower.EndsWith("-bin"))) {
            error("Invalid or reserved external_service header");
        }
        bytes += key.size() + value.size();
    }
    if (service.headers_size() > 32 || bytes > 16384) {
        error("external_service headers exceed limit");
    }
    return issues;
}

}   // namespace NFq
