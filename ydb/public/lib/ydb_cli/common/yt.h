#pragma once

#include "command.h"

#include <util/system/env.h>

namespace NYdb {
namespace NConsoleClient {

class TCommandWithYtProxy {
protected:
    template <typename TOpt>
    void ParseYtProxy(const TClientCommand::TConfig& config, const TOpt opt) {
        if (config.ParseResult->Has(opt)) {
            ParseYtProxy(config.ParseResult->Get(opt));
        } else {
            ParseYtProxy(GetEnv("YT_PROXY"));
        }
    }

    TString YtHost;
    ui16 YtPort;

private:
    void ParseYtProxy(const TString& proxy);

    static constexpr ui16 DefaultYtPort = 80;
};

class TCommandWithYtToken {
protected:
    template <typename TOpt>
    void ParseYtToken(const TClientCommand::TConfig& config, const TOpt opt) {
        if (config.ParseResult->Has(opt)) {
            YtToken = config.ParseResult->Get(opt);
        } else {
            TString token = GetEnv("YT_TOKEN");

            if (!token.empty()) {
                YtToken = std::move(token);
            } else {
                ReadYtToken();
            }
        }
    }

    TString YtToken;

    static const TString YtTokenFile;

private:
    void ReadYtToken();
};

}
}
