#include "external_service.h"
#include "external_source.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/uri/uri.h>
#include <util/string/cast.h>

namespace NKikimr::NExternalSource {
namespace {
void Require(bool condition, const char* message) {
    if (!condition) {
        ythrow TExternalSourceException() << message;
    }
}
}   // namespace

void TServiceConnectionSettings::Validate() const {
    Require(Protocol == "HTTP" || Protocol == "GRPC", "ExternalService PROTOCOL must be HTTP or GRPC");
    Require(!Endpoint.empty() && Endpoint.size() <= 4096, "Invalid ExternalService LOCATION");
    for (unsigned char c : Endpoint) {
        Require(c > 32 && c != 127, "Invalid ExternalService LOCATION");
    }
    NUri::TUri uri;
    const auto status = uri.ParseUri(Protocol == "HTTP" ? Endpoint : "http://" + Endpoint, NUri::TFeature::FeatureSchemeKnown);
    Require(status == NUri::TState::ParsedOK && !uri.GetField(NUri::TField::FieldHost).empty() && !uri.FldIsSet(NUri::TField::FieldUser) &&
                !uri.FldIsSet(NUri::TField::FieldPass) && !uri.FldIsSet(NUri::TField::FieldQuery) &&
                !uri.FldIsSet(NUri::TField::FieldFrag), "Invalid ExternalService LOCATION");
    if (Protocol == "HTTP") {
        Require(Endpoint.StartsWith(UseTls ? "https://" : "http://"), "ExternalService LOCATION scheme must match USE_TLS");
        const auto path = uri.GetField(NUri::TField::FieldPath);
        Require(path.empty() || path == "/", "ExternalService HTTP LOCATION must be an origin; WASM supplies the path");
    } else {
        ui32 port = 0;
        Require(Endpoint.find_first_of("/?#@") == TString::npos && TryFromString(uri.GetField(NUri::TField::FieldPort), port) && port > 0 &&
                    port <= 65535, "ExternalService GRPC LOCATION must be host:port");
    }
    Require(CaCertificate.size() <= 65536 && (UseTls || CaCertificate.empty()),
        "ExternalService CA_CERTIFICATE requires TLS and must not exceed 65536 bytes");
    Require((!TvmSourceId && !TvmDestinationId) || (TvmSourceId && TvmDestinationId && !SecretReference.empty()),
        "ExternalService TVM requires nonzero source/destination ids and a secret reference");
}

TServiceConnectionSettings ParseServiceConnection(const NKikimrSchemeOp::TExternalDataSourceDescription& description) {
    Require(description.GetSourceType() == "ExternalService", "Expected an ExternalService connection");
    const auto& properties = description.GetProperties().GetProperties();
    auto get = [&](const char* key, const TString& fallback = {}) -> TString {
        const auto it = properties.find(key);
        return it == properties.end() ? fallback : TString(it->second);
    };
    TServiceConnectionSettings settings;
    settings.Protocol = get("protocol");
    settings.Endpoint = description.GetLocation();
    TString tls = get("use_tls", "true");
    tls.to_lower();
    Require(tls == "true" || tls == "false", "ExternalService USE_TLS must be true or false");
    settings.UseTls = tls == "true";
    settings.CaCertificate = get("ca_certificate");
    if (description.GetAuth().has_token()) {
        settings.SecretReference = description.GetAuth().GetToken().GetTokenSecretName();
        Require(!settings.SecretReference.empty(), "ExternalService requires a secret reference");
    } else {
        Require(description.GetAuth().has_none(), "ExternalService supports NONE, TOKEN and TVM authentication");
    }
    const bool tvm = properties.contains("tvm_source_id") || properties.contains("tvm_destination_id");
    if (tvm) {
        Require(TryFromString(get("tvm_source_id"), settings.TvmSourceId) && settings.TvmSourceId &&
                    TryFromString(get("tvm_destination_id"), settings.TvmDestinationId) &&
                    settings.TvmDestinationId, "ExternalService TVM requires nonzero source/destination ids");
    }
    settings.Validate();
    return settings;
}

TString TServiceConnectionSettings::Serialize() const {
    Validate();
    NJson::TJsonValue value(NJson::JSON_MAP);
    value["protocol"] = Protocol;
    value["endpoint"] = Endpoint;
    value["tls"] = UseTls;
    value["ca"] = CaCertificate;
    value["secret_ref"] = SecretReference;
    value["tvm_source"] = TvmSourceId;
    value["tvm_destination"] = TvmDestinationId;
    return NJson::WriteJson(value, false);
}

TServiceConnectionSettings TServiceConnectionSettings::Deserialize(const TString& serialized) {
    NJson::TJsonValue value;
    Require(NJson::ReadJsonTree(serialized, &value) && value.IsMap(), "Invalid ExternalService settings");
    TServiceConnectionSettings settings;
    settings.Protocol = value["protocol"].GetStringSafe();
    settings.Endpoint = value["endpoint"].GetStringSafe();
    settings.UseTls = value["tls"].GetBooleanSafe();
    settings.CaCertificate = value["ca"].GetStringSafe();
    settings.SecretReference = value["secret_ref"].GetStringSafe();
    const auto source = value["tvm_source"].GetUIntegerSafe();
    const auto destination = value["tvm_destination"].GetUIntegerSafe();
    Require(source <= Max<ui32>() && destination <= Max<ui32>(), "Invalid ExternalService TVM ids");
    settings.TvmSourceId = source;
    settings.TvmDestinationId = destination;
    settings.Validate();
    return settings;
}

}   // namespace NKikimr::NExternalSource
