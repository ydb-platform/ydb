#include "mvp_startup_options.h"
#include "utils.h"

#include <library/cpp/protobuf/json/json2proto.h>
#include <util/stream/file.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/system/hostname.h>
#include <ydb/library/yaml_json/yaml_to_json.h>

#include <google/protobuf/text_format.h>

#include <iostream>
#include <yaml-cpp/yaml.h>

namespace NMVP {
namespace {

template <typename TMessage, typename TNormalizeNameFn>
void MergeRepeatedByName(google::protobuf::RepeatedPtrField<TMessage>* dst,
                         const google::protobuf::RepeatedPtrField<TMessage>& src,
                         TNormalizeNameFn&& normalizeName)
{
    for (const auto& srcItem : src) {
        TMessage normalizedItem = srcItem;
        normalizeName(&normalizedItem);

        const TString& name = normalizedItem.GetName();
        if (name.empty()) {
            dst->Add()->MergeFrom(normalizedItem);
            continue;
        }

        TMessage* target = nullptr;
        for (auto& dstItem : *dst) {
            if (dstItem.GetName() == name) {
                target = &dstItem;
                break;
            }
        }

        if (target == nullptr) {
            target = dst->Add();
            target->SetName(name);
        }

        target->MergeFrom(normalizedItem);
        target->SetName(name);
    }
}

template <typename TMessage>
void MergeRepeatedByName(google::protobuf::RepeatedPtrField<TMessage>* dst,
                         const google::protobuf::RepeatedPtrField<TMessage>& src)
{
    MergeRepeatedByName(dst, src, [](TMessage*) {});
}

NProtobufJson::TJson2ProtoConfig MakeJson2ProtoConfig() {
    return NProtobufJson::TJson2ProtoConfig()
        .SetFieldNameMode(NProtobufJson::TJson2ProtoConfig::FieldNameSnakeCaseDense)
        .SetEnumValueMode(NProtobufJson::TJson2ProtoConfig::EnumCaseInsensetive)
        .SetAllowUnknownFields(true);
}

void MergeYamlNodeToProto(const YAML::Node& node, google::protobuf::Message& proto) {
    if (!node || !node.IsDefined() || node.IsNull()) {
        return;
    }

    const NJson::TJsonValue json = NKikimr::NYaml::Yaml2Json(node, true);
    NProtobufJson::MergeJson2Proto(json, proto, MakeJson2ProtoConfig());
}

} // namespace

TMvpStartupOptions TMvpStartupOptions::Build(int argc, const char* argv[]) {
    TMvpStartupOptions startupOptions;

    NLastGetopt::TOptsParseResult parsedArgs = startupOptions.ParseArgs(argc, argv);
    startupOptions.LoadConfig(parsedArgs);
    startupOptions.SetPorts();
    startupOptions.LoadTokens();
    startupOptions.OverrideTokensConfig();
    startupOptions.LoadCertificates();

    return startupOptions;
}

TString TMvpStartupOptions::GetLocalEndpoint() const {
    if (!HttpPort && !HttpsPort) {
        ythrow yexception() << "At least one of HTTP or HTTPS ports must be specified";
    }

    return HttpsPort
        ? TStringBuilder() << "https://" << FQDNHostName() << ":" << HttpsPort
        : TStringBuilder() << "http://" << FQDNHostName() << ":" << HttpPort;
}

const TString& TMvpStartupOptions::GetYamlConfigPath() const {
    return YamlConfigPath;
}

NLastGetopt::TOptsParseResult TMvpStartupOptions::ParseArgs(int argc, const char* argv[]) {
    // Opts must live longer than ParseArgs(), because the parse result refers to it.
    Opts = NLastGetopt::TOpts::Default();

    Opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&LogToStderr);
    Opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&Mlock);
    Opts.AddLongOption("config", "Path to configuration YAML file").RequiredArgument("PATH").StoreResult(&YamlConfigPath);
    Opts.AddLongOption("http-port", "HTTP port. Default " + ToString(DEFAULT_HTTP_PORT)).StoreResult(&HttpPort);
    Opts.AddLongOption("https-port", "HTTPS port. Default " + ToString(DEFAULT_HTTPS_PORT)).StoreResult(&HttpsPort);

    return NLastGetopt::TOptsParseResult(&Opts, argc, argv);
}

void TMvpStartupOptions::LoadConfig(const NLastGetopt::TOptsParseResult& parsedArgs) {
    if (!YamlConfigPath.empty()) {
        try {
            YAML::Node config = YAML::LoadFile(YamlConfigPath);
            NMvp::TMvpAppConfig appConfig;
            MergeYamlNodeToProto(config, appConfig);
            if (!appConfig.HasGeneric()) {
                return;
            }

            const NMvp::TGenericConfig& generic = appConfig.GetGeneric();
            if (!generic.IsInitialized()) {
                ythrow yexception() << "Error parsing YAML configuration file: invalid generic config";
            }
            TryGetStartupOptionsFromConfig(parsedArgs, generic);
        } catch (const YAML::Exception& e) {
            std::cerr << "Error parsing YAML configuration file: " << e.what() << std::endl;
            std::exit(EXIT_FAILURE);
        }
    }
}

void TMvpStartupOptions::TryGetStartupOptionsFromConfig(const NLastGetopt::TOptsParseResult& parsedArgs, const NMvp::TGenericConfig& generic) {
    if (generic.HasAccessServiceType()) {
        AccessServiceType = generic.GetAccessServiceType();
    }

    if (generic.HasLogging()
        && generic.GetLogging().HasStderr()
        && parsedArgs.FindLongOptParseResult("stderr") == nullptr)
    {
        LogToStderr = generic.GetLogging().GetStderr();
    }

    if (parsedArgs.FindLongOptParseResult("mlock") == nullptr && generic.HasMlock()) {
        Mlock = generic.GetMlock();
    }

    if (generic.HasAuth()) {
        const auto& auth = generic.GetAuth();
        if (auth.HasTokenFile()) {
            YdbTokenFile = auth.GetTokenFile();
        }

        if (auth.HasTokens()) {
            if (AccessServiceType != NMvp::nebius_v1) {
                ythrow yexception() << "auth.tokens overrides are only supported for Nebius access service type";
            }
            TokensOverrideConfig = auth.GetTokens();
        }
    }

    if (generic.HasServer()) {
        const auto& server = generic.GetServer();
        if (server.HasCaCertFile()) {
            CaCertificateFile = server.GetCaCertFile();
        }
        if (server.HasSslCertFile()) {
            SslCertificateFile = server.GetSslCertFile();
        }

        if (parsedArgs.FindLongOptParseResult("http-port") == nullptr && server.HasHttpPort()) {
            HttpPort = static_cast<ui16>(server.GetHttpPort());
        }

        if (parsedArgs.FindLongOptParseResult("https-port") == nullptr && server.HasHttpsPort()) {
            HttpsPort = static_cast<ui16>(server.GetHttpsPort());
        }
    }
}

void TMvpStartupOptions::SetPorts() {
    if (HttpsPort) {
        if (SslCertificateFile.empty()) {
            ythrow yexception() << "SSL certificate file must be provided for HTTPS";
        }
    }

    if (!HttpsPort && !SslCertificateFile.empty()) {
        HttpsPort = DEFAULT_HTTPS_PORT;
    }

    if (!HttpPort && !HttpsPort) {
        HttpPort = DEFAULT_HTTP_PORT;
    }
}

TString TMvpStartupOptions::AddSchemeToUserToken(const TString& token, const TString& scheme) {
    if (token.empty() || token.find(' ') != TString::npos) {
        return token;
    }
    return scheme + " " + token;
}

void TMvpStartupOptions::LoadTokens() {
    Tokens.SetAccessServiceType(AccessServiceType);

    if (YdbTokenFile.empty()) {
        return;
    }

    if (google::protobuf::TextFormat::ParseFromString(TUnbufferedFileInput(YdbTokenFile).ReadAll(), &Tokens)) {
        if (Tokens.HasStaffApiUserTokenInfo()) {
            UserToken = Tokens.GetStaffApiUserTokenInfo().GetToken();
        } else if (Tokens.HasStaffApiUserToken()) {
            UserToken = Tokens.GetStaffApiUserToken();
        }
        UserToken = AddSchemeToUserToken(UserToken, "OAuth");
    } else {
        ythrow yexception() << "Invalid ydb token file format";
    }
}

void TMvpStartupOptions::OverrideTokensConfig() {
    if (!TokensOverrideConfig.has_value()) {
        return;
    }

    const auto& override = *TokensOverrideConfig;

    if (override.HasStaffApiUserToken()) {
        Tokens.SetStaffApiUserToken(override.GetStaffApiUserToken());
    }
    if (override.HasStaffApiUserTokenInfo()) {
        Tokens.MutableStaffApiUserTokenInfo()->MergeFrom(override.GetStaffApiUserTokenInfo());
    }
    if (override.HasAccessServiceType()) {
        Tokens.SetAccessServiceType(override.GetAccessServiceType());
    }

    MergeRepeatedByName(Tokens.MutableJwtInfo(), override.GetJwtInfo());
    MergeRepeatedByName(Tokens.MutableOAuthInfo(), override.GetOAuthInfo());
    MergeRepeatedByName(Tokens.MutableSecretInfo(), override.GetSecretInfo());
    MergeRepeatedByName(Tokens.MutableMetadataTokenInfo(), override.GetMetadataTokenInfo());
    MergeRepeatedByName(Tokens.MutableStaticCredentialsInfo(), override.GetStaticCredentialsInfo());
    MergeRepeatedByName(Tokens.MutableOAuthExchange(), override.GetOAuthExchange());

    THashSet<TString> overriddenTokenExchangeNames;
    Oauth2TokenExchangeTokenName.clear();
    for (size_t i = 0; i < static_cast<size_t>(override.OAuthExchangeSize()); ++i) {
        const auto& tokenExchangeInfo = override.GetOAuthExchange(static_cast<int>(i));
        if (tokenExchangeInfo.GetName().empty()) {
            ythrow yexception() << "Configuration error: 'name' must be specified in 'auth.tokens.oauth_exchange'.";
        }
        if (i == 0) {
            Oauth2TokenExchangeTokenName = tokenExchangeInfo.GetName();
        }
        overriddenTokenExchangeNames.insert(tokenExchangeInfo.GetName());
    }

    for (const auto& tokenExchangeInfo : Tokens.GetOAuthExchange()) {
        if (overriddenTokenExchangeNames.find(tokenExchangeInfo.GetName()) == overriddenTokenExchangeNames.end()) {
            continue;
        }
        if (tokenExchangeInfo.GetTokenEndpoint().empty()) {
            ythrow yexception() << "Configuration error: 'token-endpoint' must be specified in 'auth.tokens.oauth_exchange'.";
        }
    }

    for (const auto& tokenExchangeInfo : Tokens.GetOAuthExchange()) {
        if (tokenExchangeInfo.GetTokenEndpoint().empty()) {
            continue;
        }
        if (to_lower(tokenExchangeInfo.GetTokenEndpoint()).StartsWith("http://")) {
            ythrow yexception() << "Configuration error: 'token-endpoint' must not use 'http' scheme in 'auth.tokens.oauth_exchange'.";
        }
    }
}

void TMvpStartupOptions::LoadCertificates() {
    if (!CaCertificateFile.empty()) {
        CaCertificate = TUnbufferedFileInput(CaCertificateFile).ReadAll();
        if (CaCertificate.empty()) {
            ythrow yexception() << "Invalid CA certificate file";
        }
    }
    if (!SslCertificateFile.empty()) {
        SslCertificate = TUnbufferedFileInput(SslCertificateFile).ReadAll();
        if (SslCertificate.empty()) {
            ythrow yexception() << "Invalid SSL certificate file";
        }
    }
}

} // namespace NMVP
