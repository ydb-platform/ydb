#include "root.h"
#include "cert_format_converter.h"
#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>
#include <util/system/env.h>

namespace NYdb {
namespace NConsoleClient {

const TString defaultTokenFile = "~/.ydb/token";

TClientCommandRootBase::TClientCommandRootBase(const TString& name)
    : TClientCommandTree(name)
{}

void TClientCommandRootBase::Config(TConfig& config) {
    TClientCommandTree::Config(config);
    TimeRequests = false;
    ProgressRequests = false;

    TClientCommandOptions& opts = *config.Opts;
    opts.AddLongOption('t', "time", "Show request execution time").StoreTrue(&TimeRequests);
    opts.AddLongOption('o', "progress", "Show progress of long requests").StoreTrue(&ProgressRequests);
    opts.AddLongOption("ca-file",
        "File containing PEM encoded root certificates for SSL/TLS connections.\n"
        "If this parameter is empty, the default roots will be used")
        .FileName("CA certificates").ProfileParam("ca-file", true)
        .LogToConnectionParams("ca-file")
        .Env("YDB_CA_FILE", true, "CA certificates")
        .RequiredArgument("PATH").StoreFilePath(&config.CaCertsFile).StoreResult(&config.CaCerts);
    opts.AddLongOption("client-cert-file",
        "File containing client certificate for SSL/TLS connections (PKCS#12 or PEM-encoded)")
        .FileName("Client certificate")
        .LogToConnectionParams("client-cert-file")
        .Env("YDB_CLIENT_CERT_FILE", true, "Client certificate")
        .ProfileParam("client-cert-file", true)
        .RequiredArgument("PATH").StoreFilePath(&config.ClientCertFile).StoreResult(&config.ClientCert);
    opts.AddLongOption("client-cert-key-file",
        "File containing PEM encoded client certificate private key for SSL/TLS connections")
        .FileName("Client certificate private key")
        .LogToConnectionParams("client-cert-key-file")
        .Env("YDB_CLIENT_CERT_KEY_FILE", true, "Client certificate private key")
        .ProfileParam("client-cert-key-file", true)
        .RequiredArgument("PATH").StoreFilePath(&config.ClientCertPrivateKeyFile).StoreResult(&config.ClientCertPrivateKey);
    opts.AddLongOption("client-cert-key-password-file",
        "File containing password for client certificate private key (if key is encrypted).\n"
        "If key file is encrypted, but this option is not set, password will be asked interactively")
        .FileName("Client certificate private key password")
        .LogToConnectionParams("client-cert-key-password-file")
        .Env("YDB_CLIENT_CERT_KEY_PASSWORD", false)
        .Env("YDB_CLIENT_CERT_KEY_PASSWORD_FILE", true, "Client certificate private key password")
        .ProfileParam("client-cert-key-password-file", true)
        .RequiredArgument("PATH").StoreFilePath(&config.ClientCertPrivateKeyPasswordFile).StoreResult(&config.ClientCertPrivateKeyPassword);

    opts.SetCustomUsage(config.ArgV[0]);
    config.SetFreeArgsMin(1);
    opts.GetOpts().ArgPermutation_ = NLastGetopt::REQUIRE_ORDER;
}

void TClientCommandRootBase::SetCustomUsage(TConfig& config) {
    Y_UNUSED(config);
}

void TClientCommandRootBase::Parse(TConfig& config) {
    TClientCommandTree::Parse(config);

    TClientCommand::TIME_REQUESTS = TimeRequests;
    TClientCommand::PROGRESS_REQUESTS = ProgressRequests;
}

void TClientCommandRootBase::ParseToken(TString& token, TString& tokenFile, const TString& envName, bool useDefaultToken) {
    if (token.empty()) {
        if (tokenFile) {
            // 1. command line token-file option
            token = ReadFromFile(tokenFile, "token");
        } else {
            // 2. Environment variable
            TString ydbToken = GetEnv(envName);
            if (!ydbToken.empty()) {
                token = ydbToken;
            } else {
                if (useDefaultToken && token.empty()) {
                    // 3. Default token file
                    tokenFile = defaultTokenFile;
                    ReadFromFileIfExists(tokenFile, "default token", token);
                }
            }
        }
    }
}

bool TClientCommandRootBase::ParseProtocol(TConfig& config, TString& message) {
    auto separator_pos = Address.find("://");
    if (separator_pos != TString::npos) {
        TString protocol = Address.substr(0, separator_pos);
        protocol.to_lower();
        if (protocol == "grpcs") {
            config.EnableSsl = true;
        } else if (protocol == "grpc") {
            config.EnableSsl = false;
        } else {
            message = TStringBuilder() << "Unknown protocol \"" << protocol << "\".";
            return false;
        }
        Address = Address.substr(separator_pos + 3);
    }
    return true;
}

void TClientCommandRootBase::ParseCaCerts(TConfig& config) {
    if (!config.EnableSsl && config.CaCerts) {
        throw TMisuseException()
            << "\"ca-file\" option provided for a non-ssl connection. Use grpcs:// prefix for host to connect using SSL.";
    }
}

void TClientCommandRootBase::ParseClientCert(TConfig& config) {
    if (!config.EnableSsl && config.ClientCert) {
        TMisuseException()
            << "\"client-cert-file\"/\"client-cert-key-file\"/\"client-cert-key-password-file\" options are provided for a non-ssl connection. Use grpcs:// prefix for host to connect using SSL.";
    }

    if (config.ClientCert) {
        // Convert certificates from PKCS#12 to PEM or encrypted private key to nonencrypted
        // May ask for password
        std::tie(config.ClientCert, config.ClientCertPrivateKey) = ConvertCertToPEM(config.ClientCert, config.ClientCertPrivateKey, config.ClientCertPrivateKeyPassword);
    }
}

void TClientCommandRootBase::ParseCredentials(TConfig& config) {
    ParseToken(Token, TokenFile, "YDB_TOKEN", true);
    if (!Token.empty()) {
        config.SecurityToken = Token;
    }
}

void TClientCommandRootBase::SetFreeArgs(TConfig& config) {
    Y_UNUSED(config);
}

}
}
