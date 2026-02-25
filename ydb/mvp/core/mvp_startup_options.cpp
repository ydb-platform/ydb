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

} // namespace

TMvpStartupOptions TMvpStartupOptions::Build(int argc, const char* argv[]) {
    TMvpStartupOptions startupOptions;

    NLastGetopt::TOptsParseResult parsedArgs = startupOptions.ParseArgs(argc, argv);
    startupOptions.LoadConfig(parsedArgs);
    startupOptions.SetPorts();
    startupOptions.LoadTokens();
    startupOptions.OverrideTokensConfig();
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
            ValidateTokensOverrideConfig(auth.GetTokens());
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

void TMvpStartupOptions::LoadTokens() {
    Tokens.SetAccessServiceType(AccessServiceType);

    if (YdbTokenFile.empty()) {
        return;
    }

    if (google::protobuf::TextFormat::ParseFromString(TUnbufferedFileInput(YdbTokenFile).ReadAll(), &Tokens)) {
        MigrateJwtInfoToOAuth2Exchange();
        ValidateOAuth2ExchangeTokenNames(Tokens.GetOAuth2Exchange(), "token file config");
        ValidateOAuth2ExchangeTokenEndpointScheme(Tokens.GetOAuth2Exchange(), "token file config");
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
        if (override.GetAccessServiceType() != AccessServiceType) {
            ythrow yexception() << CONFIG_ERROR_PREFIX
                                << "auth.tokens.access_service_type must match access_service_type";
        }
    }

    MergeRepeatedByName(Tokens.MutableJwtInfo(), override.GetJwtInfo());
    MergeRepeatedByName(Tokens.MutableOAuthInfo(), override.GetOAuthInfo());
    MergeRepeatedByName(Tokens.MutableSecretInfo(), override.GetSecretInfo());
    MergeRepeatedByName(Tokens.MutableMetadataTokenInfo(), override.GetMetadataTokenInfo());
    MergeRepeatedByName(Tokens.MutableStaticCredentialsInfo(), override.GetStaticCredentialsInfo());
    MergeRepeatedByName(Tokens.MutableOAuth2Exchange(), override.GetOAuth2Exchange());
    MigrateJwtInfoToOAuth2Exchange();
    ValidateOAuth2ExchangeTokenNames(Tokens.GetOAuth2Exchange(), "merged oauth2_exchange token config");
    Tokens.SetAccessServiceType(AccessServiceType);
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
