#pragma once 
 
#include "command.h" 
 
#include <util/generic/maybe.h> 
#include <util/system/env.h> 
 
namespace NYdb { 
namespace NConsoleClient { 
 
class TCommandWithAwsCredentials { 
    template <typename TOpt, typename TReaderFunc> 
    static void Parse( 
            const TClientCommand::TConfig& config, 
            const TOpt opt, 
            const TString& envKey, 
            TString& parsed, 
            TReaderFunc readerFunc) { 
 
        if (config.ParseResult->Has(opt)) { 
            parsed = config.ParseResult->Get(opt); 
        } else { 
            TString fromEnv = GetEnv(envKey); 
 
            if (!fromEnv.empty()) { 
                parsed = std::move(fromEnv); 
            } else { 
                readerFunc(); 
            } 
        } 
    } 
 
protected: 
    template <typename TOpt> 
    void ParseAwsAccessKey(const TClientCommand::TConfig& config, const TOpt opt) { 
        Parse(config, opt, "AWS_ACCESS_KEY_ID", AwsAccessKey, [this]() { 
            ReadAwsAccessKey(); 
        }); 
    } 
 
    template <typename TOpt> 
    void ParseAwsSecretKey(const TClientCommand::TConfig& config, const TOpt opt) { 
        Parse(config, opt, "AWS_SECRET_ACCESS_KEY", AwsSecretKey, [this]() { 
            ReadAwsSecretKey(); 
        }); 
    } 
 
    TString AwsAccessKey; 
    TString AwsSecretKey; 
 
    static const TString AwsCredentialsFile; 
 
private: 
    const TString& ReadAwsCredentialsFile(); 
    void ReadAwsAccessKey(); 
    void ReadAwsSecretKey(); 
 
    TMaybe<TString> FileContent; 
}; 
 
} 
} 
