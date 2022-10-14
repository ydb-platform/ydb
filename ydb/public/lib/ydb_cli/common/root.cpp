#include "root.h"
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

    NLastGetopt::TOpts& opts = *config.Opts;
    opts.AddLongOption('t', "time", "Show request execution time").NoArgument().SetFlag(&TimeRequests);
    opts.AddLongOption('o', "progress", "Show progress of long requests").NoArgument().SetFlag(&ProgressRequests);
    opts.AddLongOption("ca-file",
        "Path to a file containing the PEM encoding of the server root certificates for tls connections.\n"
        "If this parameter is empty, the default roots will be used.")
        .RequiredArgument("PATH").StoreResult(&CaCertsFile);

    opts.SetCustomUsage(config.ArgV[0]);
    config.SetFreeArgsMin(1);
    opts.ArgPermutation_ = NLastGetopt::REQUIRE_ORDER;
}

void TClientCommandRootBase::SetCustomUsage(TConfig& config) {
    Y_UNUSED(config);
}

void TClientCommandRootBase::Parse(TConfig& config) {
    TClientCommandTree::Parse(config);
    ParseCredentials(config);
    ParseAddress(config);

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
    if (CaCertsFile.empty()) {
        return;
    }
    if (!config.EnableSsl) {
        throw TMisuseException()
            << "\"ca-file\" option provided for a non-ssl connection. Use grpcs:// prefix for host to connect using SSL.";
    }
    config.CaCerts = ReadFromFile(CaCertsFile, "CA certificates");
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
