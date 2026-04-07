#include "mvp_startup_options.h"
#include "utils.h"

#include <google/protobuf/text_format.h>
#include <yaml-cpp/yaml.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/hostname.h>

#include <iostream>

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

void EnsureAccessServiceTypeMatch(const std::optional<NMvp::EAccessServiceType>& left,
                                  const std::optional<NMvp::EAccessServiceType>& right,
                                  TStringBuf message)
{
    if (left && right && *left != *right) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << message;
    }
}

} // namespace

TMvpStartupOptions TMvpStartupOptions::Build(int argc, const char* argv[]) {
    TMvpStartupOptions startupOptions;

    NLastGetopt::TOptsParseResult parsedArgs = startupOptions.ParseArgs(argc, argv);
    startupOptions.LoadConfig(parsedArgs);
    startupOptions.SetPorts();
    startupOptions.LoadTokensFromTokenFile();
    startupOptions.OverrideTokensFromConfig();
    startupOptions.MergeAccessServiceType();
    startupOptions.MigrateJwtInfoToOAuth2Exchange();
    startupOptions.ValidateOAuth2ExchangeTokenNames(startupOptions.Tokens.GetOAuth2Exchange(), "token file config");
    startupOptions.ValidateOAuth2ExchangeTokenEndpointScheme(startupOptions.Tokens.GetOAuth2Exchange(), "token file config");
    startupOptions.ValidateTokensConfig();
    startupOptions.LoadCertificates();

    return startupOptions;
}

TString TMvpStartupOptions::GetLocalEndpoint() const {
    if (!HttpPort && !HttpsPort) {
        ythrow yexception() << CONFIG_ERROR_PREFIX << "At least one of HTTP or HTTPS ports must be specified";
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
            YAML::Node genericNode = config["generic"];
            NMvp::TGenericConfig generic;
            if (genericNode) {
                MergeYamlNodeToProto(genericNode, generic);
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
        AccessServiceTypeFromConfig = generic.GetAccessServiceType();
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
            const auto& tokensFromConfig = auth.GetTokens();
            if (tokensFromConfig.HasAccessServiceType()) {
                EnsureAccessServiceTypeMatch(
                    AccessServiceTypeFromConfig,
                    std::make_optional(tokensFromConfig.GetAccessServiceType()),
                    "auth.tokens.access_service_type must match access_service_type");
                AccessServiceTypeFromConfig = tokensFromConfig.GetAccessServiceType();
            }

            ValidateTokensFromConfig(tokensFromConfig);
            TokensFromConfig = tokensFromConfig;
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
            ythrow yexception() << CONFIG_ERROR_PREFIX << "SSL certificate file must be provided for HTTPS";
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

void TMvpStartupOptions::LoadTokensFromTokenFile() {
    if (!YdbTokenFile.empty()) {
        if (google::protobuf::TextFormat::ParseFromString(TUnbufferedFileInput(YdbTokenFile).ReadAll(), &Tokens)) {
            if (Tokens.HasAccessServiceType()) {
                AccessServiceTypeFromTokenFile = Tokens.GetAccessServiceType();
            }
            if (Tokens.HasStaffApiUserTokenInfo()) {
                UserToken = Tokens.GetStaffApiUserTokenInfo().GetToken();
            } else if (Tokens.HasStaffApiUserToken()) {
                UserToken = Tokens.GetStaffApiUserToken();
            }
            UserToken = AddSchemeToUserToken(UserToken, "OAuth");
        } else {
            ythrow yexception() << CONFIG_ERROR_PREFIX << "Invalid ydb token file format";
        }
    }
}

void TMvpStartupOptions::MergeAccessServiceType() {
    EnsureAccessServiceTypeMatch(AccessServiceTypeFromConfig,
                                 AccessServiceTypeFromTokenFile,
                                 "token file access_service_type must match access_service_type");

    AccessServiceType = AccessServiceTypeFromConfig.value_or(
        AccessServiceTypeFromTokenFile.value_or(NMvp::yandex_v2));

    Tokens.SetAccessServiceType(AccessServiceType);
}

void TMvpStartupOptions::OverrideTokensFromConfig() {
    if (!TokensFromConfig.has_value()) {
        return;
    }

    const auto& override = *TokensFromConfig;

    if (override.HasStaffApiUserToken()) {
        Tokens.SetStaffApiUserToken(override.GetStaffApiUserToken());
    }
    if (override.HasStaffApiUserTokenInfo()) {
        Tokens.MutableStaffApiUserTokenInfo()->MergeFrom(override.GetStaffApiUserTokenInfo());
    }

    MergeRepeatedByName(Tokens.MutableJwtInfo(), override.GetJwtInfo());
    MergeRepeatedByName(Tokens.MutableOAuthInfo(), override.GetOAuthInfo());
    MergeRepeatedByName(Tokens.MutableSecretInfo(), override.GetSecretInfo());
    MergeRepeatedByName(Tokens.MutableMetadataTokenInfo(), override.GetMetadataTokenInfo());
    MergeRepeatedByName(Tokens.MutableStaticCredentialsInfo(), override.GetStaticCredentialsInfo());
    MergeRepeatedByName(Tokens.MutableOAuth2Exchange(), override.GetOAuth2Exchange());
}

void TMvpStartupOptions::LoadCertificates() {
    if (!CaCertificateFile.empty()) {
        CaCertificate = TUnbufferedFileInput(CaCertificateFile).ReadAll();
        if (CaCertificate.empty()) {
            ythrow yexception() << CONFIG_ERROR_PREFIX << "Invalid CA certificate file";
        }
    }
    if (!SslCertificateFile.empty()) {
        SslCertificate = TUnbufferedFileInput(SslCertificateFile).ReadAll();
        if (SslCertificate.empty()) {
            ythrow yexception() << CONFIG_ERROR_PREFIX << "Invalid SSL certificate file";
        }
    }
}

} // namespace NMVP
