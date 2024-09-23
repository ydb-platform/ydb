#pragma once

#include "common.h"

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange/from_file.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/colorizer/colors.h>
#include <library/cpp/logger/priority.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/charset/utf8.h>
#include <util/string/type.h>
#include <string>

namespace NYdb {
namespace NConsoleClient {

class TClientCommand {
public:
    static bool TIME_REQUESTS; // measure time of requests
    static bool PROGRESS_REQUESTS; // display progress of long requests
    TString Name;
    TVector<TString> Aliases;
    TString Description;
    bool Visible = true;
    const TClientCommand* Parent;
    NLastGetopt::TOpts Opts;
    TString Argument;
    TMap<ui32, TString> Args;

    TClientCommand(
        const TString& name,
        const std::initializer_list<TString>& aliases = std::initializer_list<TString>(),
        const TString& description = TString(),
        bool visible = true);

    class TConfig {
        struct TCommandInfo {
            TString Name;
            NLastGetopt::TOpts* Options;
        };

        struct TConnectionParam {
            TString Value;
            TString Source;
        };

    public:
        using TCredentialsGetter = std::function<std::shared_ptr<ICredentialsProviderFactory>(const TClientCommand::TConfig&)>;

        class TArgSetting {
        public:
            void Set(size_t value) {
                Value = value;
                IsSet = true;
            }

            size_t Get() const {
                return Value;
            }

            bool GetIsSet() const {
                return IsSet;
            }

        private:
            size_t Value = 0;
            bool IsSet = false;
        };

        struct TArgSettings {
            TArgSetting Min;
            TArgSetting Max;
        };

        enum EVerbosityLevel : ui32 {
            NONE = 0,
            WARN = 1,
            INFO = 2,
            DEBUG = 3,
        };

        static ELogPriority VerbosityLevelToELogPriority(EVerbosityLevel lvl);

        int ArgC;
        char** ArgV;
        int InitialArgC;
        char** InitialArgV;
        NLastGetopt::TOpts* Opts;
        const NLastGetopt::TOptsParseResult* ParseResult;
        TVector<TString> Tokens;
        TString SecurityToken;
        TList<TCommandInfo> ParentCommands;
        THashSet<TString> ExecutableOptions;
        bool HasExecutableOptions = false;
        TString Path;
        THolder<TArgSettings> ArgsSettings;
        TString Address;
        TString Database;
        TString CaCerts;
        TString CaCertsFile;
        TMap<TString, TVector<TConnectionParam>> ConnectionParams;
        bool EnableSsl = false;
        bool IsNetworkIntensive = false;
        TString Oauth2KeyFile;

        EVerbosityLevel VerbosityLevel = EVerbosityLevel::NONE;
        size_t HelpCommandVerbosiltyLevel = 1; // No options -h or one - 1, -hh - 2, -hhh - 3 etc

        bool JsonUi64AsText = false;
        bool JsonBinaryAsBase64 = false;

        ui64 TabletId; // admin tablet #
        ui32 NodeId; // admin node #
        TString Tenant; // admin tenant name
        TString SlotId; // admin slot id

        TLoginCredentialsParams StaticCredentials;

        TString YCToken;
        bool UseMetadataCredentials = false;
        TString SaKeyFile;
        TString IamEndpoint;
        TString YScope;
        TString ChosenAuthMethod;

        TString ProfileFile;
        bool UseAccessToken = true;
        bool UseIamAuth = false;
        bool UseStaticCredentials = false;
        bool UseOauth2TokenExchange = false;
        bool UseExportToYt = true;
        // Whether a command needs a connection to YDB
        bool NeedToConnect = true;
        bool NeedToCheckForUpdate = true;
        bool ForceVersionCheck = false;

        TCredentialsGetter CredentialsGetter;

        TConfig(int argc, char** argv)
            : ArgC(argc)
            , ArgV(argv)
            , InitialArgC(argc)
            , InitialArgV(argv)
            , Opts(nullptr)
            , ParseResult(nullptr)
            , HelpCommandVerbosiltyLevel(ParseHelpCommandVerbosilty(argc, argv))
            , TabletId(0)
        {
            CredentialsGetter = [](const TClientCommand::TConfig& config) {
                if (config.SecurityToken) {
                    return CreateOAuthCredentialsProviderFactory(config.SecurityToken);
                }
                if (config.UseOauth2TokenExchange) {
                    if (config.Oauth2KeyFile) {
                        return CreateOauth2TokenExchangeFileCredentialsProviderFactory(config.Oauth2KeyFile, config.IamEndpoint);
                    }
                }
                return CreateInsecureCredentialsProviderFactory();
            };
        }

        bool HasHelpCommand() const {
            return HasArgs({ "--help" }) || HasArgs({ "-h" }) || HasArgs({ "-?" }) || HasArgs({ "--help-ex" });
        }

        static size_t ParseHelpCommandVerbosilty(int argc, char** argv);

        bool IsVerbose() const {
            return VerbosityLevel != EVerbosityLevel::NONE;
        }

        void SetFreeArgsMin(size_t value) {
            ArgsSettings->Min.Set(value);
            Opts->SetFreeArgsMin(value);
        }

        void SetFreeArgsMax(size_t value) {
            ArgsSettings->Max.Set(value);
            Opts->SetFreeArgsMax(value);
        }

        void SetFreeArgsNum(size_t minValue, size_t maxValue) {
            ArgsSettings->Min.Set(minValue);
            ArgsSettings->Max.Set(maxValue);
            Opts->SetFreeArgsNum(minValue, maxValue);
        }

        void SetFreeArgsNum(size_t value) {
            SetFreeArgsNum(value, value);
        }

        void CheckParamsCount() {
            size_t count = GetParamsCount();
            if (HasHelpCommand() || HasExecutableOptions) {
                return;
            }
            bool minSet = ArgsSettings->Min.GetIsSet();
            size_t minValue = ArgsSettings->Min.Get();
            bool maxSet = ArgsSettings->Max.GetIsSet();
            size_t maxValue = ArgsSettings->Max.Get();
            bool minFailed = minSet && count < minValue;
            bool maxFailed = maxSet && count > maxValue;
            if (minFailed || maxFailed) {
                if (minSet && maxSet) {
                    if (minValue == maxValue) {
                        throw TMisuseException() << "Command " << ArgV[0]
                            << " requires exactly " << minValue << " free arg(s).";
                    }
                    throw TMisuseException() << "Command " << ArgV[0]
                        << " requires from " << minValue << " to " << maxValue << " free arg(s).";
                }
                if (minFailed) {
                    throw TMisuseException() << "Command " << ArgV[0]
                        << " requires at least " << minValue << " free arg(s).";
                }
                throw TMisuseException() << "Command " << ArgV[0]
                    << " requires at most " << maxValue << " free arg(s).";
            }
        }

    private:
        size_t GetParamsCount() {
            size_t result = 0;
            bool optionArgument = false;
            for (int i = 1; i < ArgC; ++i) {
                char* pos = ArgV[i];
                while (*pos == '\"' || *pos == '\'') {
                    ++pos;
                }
                if (*pos == '-') {
                    optionArgument = true;
                    // Exclude --opt=value  case
                    while (*pos != '\0') {
                        if (*pos == '=') {
                            optionArgument = false;
                            break;
                        }
                        ++pos;
                    }
                    // Exclude opts with no arguments
                    const NLastGetopt::TOpt* opt;
                    pos = ArgV[i] + 1;
                    if (*pos == '-') {
                        char* end = pos + 1;
                        while (*end != '\0' && *end != '\'' && *end != '\"') {
                            ++end;
                        }
                        opt = Opts->FindLongOption(TString(pos + 1, end));
                    } else {
                        opt = Opts->FindCharOption(*pos);
                    }
                    if (opt && opt->GetHasArg() == NLastGetopt::NO_ARGUMENT) {
                        optionArgument = false;
                    }
                } else {
                    if (optionArgument) {
                        optionArgument = false;
                    } else {
                        ++result;
                    }
                }
            }
            return result;
        }

        bool HasArgs(const std::vector<TString>& args) const {
            for (const auto& arg : args) {
                bool found = false;
                for (int i = 0; i < InitialArgC; ++i) {
                    if (InitialArgV[i] == arg) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return false;
                }
            }
            return true;
        }
    };

    class TOptsParseOneLevelResult : public NLastGetopt::TOptsParseResult {
    public:
        TOptsParseOneLevelResult(TConfig& config);
    };

    virtual ~TClientCommand() {}

    virtual int Process(TConfig& config);
    virtual void Prepare(TConfig& config);
    virtual int ValidateAndRun(TConfig& config);

    enum RenderEntryType {
        BEGIN,
        MIDDLE,
        END
    };

    void RenderOneCommandDescription(
        TStringStream& stream,
        const NColorizer::TColors& colors = NColorizer::TColors(false),
        RenderEntryType type = BEGIN
    );

    void Hide();

protected:
    virtual void Config(TConfig& config);
    virtual void SaveParseResult(TConfig& config);
    virtual void Parse(TConfig& config);
    virtual void Validate(TConfig& config);
    virtual int Run(TConfig& config);

    void SetFreeArgTitle(size_t pos, const TString& title, const TString& help);
    virtual void SetCustomUsage(TConfig& config);

protected:
    std::shared_ptr<NLastGetopt::TOptsParseResult> ParseResult;

private:
    void HideOption(const TString& name);
    void ChangeOptionDescription(const TString& name, const TString& description);
    void CheckForExecutableOptions(TConfig& config);

    constexpr static int DESCRIPTION_ALIGNMENT = 28;
    bool Hidden = false;
};

class TClientCommandTree : public TClientCommand {
public:
    TClientCommandTree(const TString& name, const std::initializer_list<TString>& aliases = std::initializer_list<TString>(), const TString& description = TString());
    void AddCommand(std::unique_ptr<TClientCommand> command);
    void AddHiddenCommand(std::unique_ptr<TClientCommand> command);
    virtual void Prepare(TConfig& config) override;
    void RenderCommandsDescription(
        TStringStream& stream,
        const NColorizer::TColors& colors = NColorizer::TColors(false)
    );
    virtual void SetFreeArgs(TConfig& config);
    bool HasSelectedCommand() const { return SelectedCommand; }

protected:
    virtual void Config(TConfig& config) override;
    virtual void SaveParseResult(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;

    TClientCommand* SelectedCommand;

private:
    bool HasOptionsToShow();

    TMap<TString, std::unique_ptr<TClientCommand>> SubCommands;
    TMap<TString, TString> Aliases;
};

class TCommandWithPath {
protected:
    // Get path from free argument and adjust it
    void ParsePath(const TClientCommand::TConfig& config, const size_t argPos, bool isPathOptional = false);
    void AdjustPath(const TClientCommand::TConfig& config);

    TString Path;
};

class TCommandWithTopicName {
protected:
    void ParseTopicName(const TClientCommand::TConfig& config, const size_t argPos);

    TString TopicName;
};

}
}
