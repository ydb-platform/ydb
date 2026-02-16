#include "mvp_startup_options.h"

#include <util/stream/file.h>
#include <util/generic/yexception.h>
#include <util/string/strip.h>
#include <util/system/hostname.h>

#include <google/protobuf/text_format.h>

#include <iostream>

namespace NMVP {

TMvpStartupOptions TMvpStartupOptions::Build(int argc, const char* argv[]) {
    TMvpStartupOptions startupOptions;

    NLastGetopt::TOptsParseResult parsedArgs = startupOptions.ParseArgs(argc, argv);
    startupOptions.LoadConfig(parsedArgs);
    startupOptions.SetPorts();
    startupOptions.LoadTokens();
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
            Config = YAML::LoadFile(YamlConfigPath);
            TryGetStartupOptionsFromConfig(parsedArgs);
        } catch (const YAML::Exception& e) {
            std::cerr << "Error parsing YAML configuration file: " << e.what() << std::endl;
            std::exit(EXIT_FAILURE);
        }
    }
}

void TMvpStartupOptions::TryGetStartupOptionsFromConfig(const NLastGetopt::TOptsParseResult& parsedArgs) {
    if (!Config["generic"]) {
        return;
    }
    auto generic = Config["generic"];

    if (generic["logging"] && generic["logging"]["stderr"]) {
        if (parsedArgs.FindLongOptParseResult("stderr") == nullptr) {
            LogToStderr = generic["logging"]["stderr"].as<bool>(false);
        }
    }

    if (generic["mlock"]) {
        if (parsedArgs.FindLongOptParseResult("mlock") == nullptr) {
            Mlock = generic["mlock"].as<bool>(false);
        }
    }

    if (generic["auth"]) {
        auto auth = generic["auth"];
        YdbTokenFile = auth["token_file"].as<std::string>("");
    }

    if (generic["server"]) {
        auto server = generic["server"];
        CaCertificateFile = server["ca_cert_file"].as<std::string>("");
        SslCertificateFile = server["ssl_cert_file"].as<std::string>("");

        if (parsedArgs.FindLongOptParseResult("http-port") == nullptr) {
            HttpPort = server["http_port"].as<ui16>(0);
        }

        if (parsedArgs.FindLongOptParseResult("https-port") == nullptr) {
            HttpsPort = server["https_port"].as<ui16>(0);
        }
    }

    if (generic["access_service_type"]) {
        auto accessServiceTypeStr = TString(generic["access_service_type"].as<std::string>(""));
        if (!NMvp::EAccessServiceType_Parse(to_lower(accessServiceTypeStr), &AccessServiceType)) {
            ythrow yexception() << "Unknown access_service_type value: " << accessServiceTypeStr;
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
        if (!Tokens.HasAccessServiceType()) {
            Tokens.SetAccessServiceType(AccessServiceType);
        }
    } else {
        ythrow yexception() << "Invalid ydb token file format";
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
