#pragma once

#include "command.h"

namespace NYdb {
namespace NConsoleClient {

extern const TString defaultTokenFile;

class TClientCommandRootBase : public TClientCommandTree {
public:
    TClientCommandRootBase(const TString& name);

    bool TimeRequests;
    bool ProgressRequests;
    TString Address;
    bool EnableSsl = true;
    TString Token;
    TString TokenFile;

    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    void SetCustomUsage(TConfig& config) override;
    void SetFreeArgs(TConfig& config) override;

protected:
    void ParseToken(TString& token, TString& tokenFile, const TString& envName, bool useDefaultToken = false);
    bool ParseProtocol(TConfig& config, TString& message);
    virtual void ParseCaCerts(TConfig& config);
    virtual void ParseClientCert(TConfig& config);
    virtual void ParseCredentials(TConfig& config);
    virtual void ParseAddress(TConfig& config) = 0;
};

}
}
