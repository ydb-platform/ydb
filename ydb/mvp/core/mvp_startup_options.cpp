#include "mvp_startup_options.h"

#include <library/cpp/getopt/last_getopt.h>

#include <util/stream/file.h>
#include <util/generic/yexception.h>
#include <util/string/strip.h>

#include <google/protobuf/text_format.h>
#include <yaml-cpp/yaml.h>

#include <iostream>

namespace NMVP {

TMvpStartupOptions TMvpStartupOptions::Build(int argc, char** argv) {
    TMvpStartupOptions opts;

    NLastGetopt::TOptsParseResult parsedArgs = opts.ParseArgs(argc, argv);
    opts.LoadConfig(parsedArgs);
    opts.SetPorts();
    opts.LoadTokens();
    opts.LoadCertificates();

    return opts;
}

NLastGetopt::TOptsParseResult TMvpStartupOptions::ParseArgs(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    opts.AddLongOption("stderr", "Redirect log to stderr").NoArgument().SetFlag(&LogToStderr);
    opts.AddLongOption("mlock", "Lock resident memory").NoArgument().SetFlag(&Mlock);
    opts.AddLongOption("config", "Path to configuration YAML file").RequiredArgument("PATH").StoreResult(&YamlConfigPath);
    opts.AddLongOption("http-port", "HTTP port. Default " + ToString(DEFAULT_HTTP_PORT)).StoreResult(&HttpPort);
    opts.AddLongOption("https-port", "HTTPS port. Default " + ToString(DEFAULT_HTTPS_PORT)).StoreResult(&HttpsPort);

    return NLastGetopt::TOptsParseResult(&opts, argc, argv);
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
    if (HttpPort > 0) {
        Http = true;
    }
    if (HttpsPort > 0 || !SslCertificateFile.empty()) {
        Https = true;
    }
    if (!Http && !Https) {
        Http = true;
    }
    if (HttpPort == 0) {
        HttpPort = DEFAULT_HTTP_PORT;
    }
    if (HttpsPort == 0) {
        HttpsPort = DEFAULT_HTTPS_PORT;
    }
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
