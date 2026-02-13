#pragma once

#include <ydb/mvp/core/protos/mvp.pb.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <yaml-cpp/yaml.h>

namespace NLastGetopt { class TOptsParseResult; }

namespace NMVP {

struct TMvpStartupOptions {
private:
    static constexpr ui16 DEFAULT_HTTP_PORT = 8788;
    static constexpr ui16 DEFAULT_HTTPS_PORT = 8789;

    NLastGetopt::TOpts Opts;
    TString YamlConfigPath;
    TString YdbTokenFile;
    TString CaCertificateFile;
    TString SslCertificateFile;

public:
    YAML::Node Config;

    bool LogToStderr = false;
    bool Mlock = false;
    ui16 HttpPort = 0;
    ui16 HttpsPort = 0;

    TString UserToken;
    NMvp::TTokensConfig Tokens;
    TString CaCertificate;
    TString SslCertificate;
    NMvp::EAccessServiceType AccessServiceType = NMvp::yandex_v2;

    static TMvpStartupOptions Build(int argc, const char* argv[]);
    TString GetLocalEndpoint() const;

private:
    NLastGetopt::TOptsParseResult ParseArgs(int argc, const char* argv[]);
    void LoadConfig(const NLastGetopt::TOptsParseResult& parsedArgs);
    void TryGetStartupOptionsFromConfig(const NLastGetopt::TOptsParseResult& parsedArgs);
    void SetPorts();
    TString AddSchemeToUserToken(const TString& token, const TString& scheme);
    void LoadTokens();
    void LoadCertificates();
};

} // namespace NMVP
