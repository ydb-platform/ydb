#pragma once

#include "command.h"

#include <library/cpp/config/config.h>

#include <util/generic/maybe.h>
#include <util/system/env.h>

namespace NYdb::NConsoleClient {

class TCommandWithAwsCredentials {
    template <typename TOpt>
    void Parse(const TClientCommand::TConfig& config, const TOpt opt,
            const TString& envKey, const TString& iniKey, TString& value)
    {
        if (config.ParseResult->Has(opt)) {
            value = config.ParseResult->Get(opt);
        } else {
            if (auto fromEnv = GetEnv(envKey)) {
                value = std::move(fromEnv);
            } else {
                value = ReadIniKey(iniKey);
            }
        }
    }

    TString ReadIniKey(const TString& iniKey);

protected:
    template <typename TOpt>
    void ParseAwsProfile(const TClientCommand::TConfig& config, const TOpt opt) {
        if (config.ParseResult->Has(opt)) {
            AwsProfile = config.ParseResult->Get(opt);
        } else {
            if (auto fromEnv = GetEnv("AWS_PROFILE")) {
                AwsProfile = std::move(fromEnv);
            }
        }
    }

    template <typename TOpt>
    void ParseAwsAccessKey(const TClientCommand::TConfig& config, const TOpt opt) {
        Parse(config, opt, "AWS_ACCESS_KEY_ID", "aws_access_key_id", AwsAccessKey);
    }

    template <typename TOpt>
    void ParseAwsSecretKey(const TClientCommand::TConfig& config, const TOpt opt) {
        Parse(config, opt, "AWS_SECRET_ACCESS_KEY", "aws_secret_access_key", AwsSecretKey);
    }

    TString AwsAccessKey;
    TString AwsSecretKey;

    static const TString AwsCredentialsFile;
    static const TString AwsDefaultProfileName;

private:
    TMaybe<NConfig::TConfig> Config;
    TMaybe<TString> AwsProfile;
};

}
