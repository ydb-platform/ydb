#pragma once

#include "common.h"

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <library/cpp/getopt/last_getopt.h>
#include <library/cpp/colorizer/colors.h>
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
    const TClientCommand* Parent;
    NLastGetopt::TOpts Opts;
    TString Argument;
    TMap<ui32, TString> Args;

    TClientCommand(const TString& name, const std::initializer_list<TString>& aliases = std::initializer_list<TString>(), const TString& description = TString());

    class TOptsParseOneLevelResult : public NLastGetopt::TOptsParseResult {
    public:
        TOptsParseOneLevelResult(const NLastGetopt::TOpts* options, int argc, char** argv);
    };

    class TConfig {
        struct TCommandInfo {
            TString Name;
            NLastGetopt::TOpts* Options;
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

            bool  GetIsSet() const {
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

        int ArgC;
        char** ArgV;
        NLastGetopt::TOpts* Opts;
        const NLastGetopt::TOptsParseResult* ParseResult;
        TVector<TString> Tokens;
        TString SecurityToken;
        TList<TCommandInfo> ParentCommands;
        TString Path;
        THolder<TArgSettings> ArgsSettings;
        TString Address;
        TString Database;
        TString CaCerts;
        bool IsVerbose = false;
        bool EnableSsl = false;

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

        TString YdbDir;
        bool UseOAuthToken = true;
        bool UseIamAuth = false;
        bool UseStaticCredentials = false;
        bool UseExportToYt = true;

        TCredentialsGetter CredentialsGetter;

        TConfig(int argc, char** argv)
            : ArgC(argc)
            , ArgV(argv)
            , Opts(nullptr)
            , ParseResult(nullptr)
            , TabletId(0)
        {
            CredentialsGetter = [](const TClientCommand::TConfig& config) {
                if (config.SecurityToken) {
                    return CreateOAuthCredentialsProviderFactory(config.SecurityToken);
                }
                return CreateInsecureCredentialsProviderFactory();
            };
        }

        bool IsHelpCommand() {
            TString lastArg = ArgV[ArgC - 1];
            return lastArg == "--help" || lastArg == "-h" || lastArg == "-?";
        }

        bool IsSvnVersionCommand() {
            TString lastArg = ArgV[ArgC - 1];
            return lastArg == "--svnrevision" || lastArg == "-V";
        }

        bool IsVersionCommand() {
            return HasArgs({ "version" });
        }

        bool IsVersionForceCheckCommand() {
            return HasArgs({ "version", "--check" });
        }

        bool IsSetVersionCheckCommand() {
            return HasArgs({ "version", "--enable-checks" }) || HasArgs({ "version", "--disable-checks" });
        }

        bool IsUpdateCommand() {
            return HasArgs({ "update" });
        }

        bool IsInitCommand() {
            TString lastArg = ArgV[ArgC - 1];
            if (lastArg == "init" && ArgC > 1) {
                TString penultimateArg = ArgV[ArgC - 2];
                if (penultimateArg.EndsWith("ydb") || penultimateArg.EndsWith("ydb.exe")) {
                    return true;
                }
            }
            return false;
        }

        bool IsProfileCommand() {
            for (int i = 1; i < 3; ++i) {
                if (ArgC <= i) {
                    return false;
                }
                TString currentArg = ArgV[ArgC - i - 1];
                if (currentArg == "profile") {
                    return true;
                }
            }
            return false;
        }

        bool IsLicenseCommand() {
            TString lastArg = ArgV[ArgC - 1];
            return lastArg == "--license";
        }

        bool IsCreditsCommand() {
            TString lastArg = ArgV[ArgC - 1];
            return lastArg == "--credits";
        }

        bool IsHelpExCommand() {
            TString lastArg = ArgV[ArgC - 1];
            return lastArg == "--help-ex";
        }

        bool IsSystemCommand() {
            return IsHelpCommand() || IsSvnVersionCommand() || IsUpdateCommand() || IsVersionCommand()
                || IsInitCommand() || IsProfileCommand() || IsLicenseCommand() || IsCreditsCommand()
                || IsHelpExCommand();
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
            if (IsSystemCommand()) {
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

        bool HasArgs(std::vector<TString> args) {
            for (const auto& arg : args) {
                bool found = false;
                for (int i = 0; i < ArgC; ++i) {
                    if (ArgV[i] == arg) {
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

    virtual ~TClientCommand() {}

    virtual void Config(TConfig& config);
    virtual void Parse(TConfig& config);
    virtual int Run(TConfig& config);
    virtual int Process(TConfig& config);

    virtual void RenderCommandsDescription(
        TStringStream& stream,
        const NColorizer::TColors& colors = NColorizer::TColors(false),
        const std::basic_string<bool>& ends = std::basic_string<bool>()
    );

protected:
    void SetFreeArgTitle(size_t pos, const TString& title, const TString& help);
    virtual void SetCustomUsage(TConfig& config);

private:
    void HideOption(const TString& name);
    void ChangeOptionDescription(const TString& name, const TString& description);

    constexpr static int DESCRIPTION_ALIGNMENT = 28;

    static TString Ends2Prefix(const std::basic_string<bool>& ends);
};

class TClientCommandTree : public TClientCommand {
public:
    TMap<TString, std::unique_ptr<TClientCommand>> SubCommands;
    TMap<TString, TString> Aliases;
    TClientCommand* SelectedCommand;

    TClientCommandTree(const TString& name, const std::initializer_list<TString>& aliases = std::initializer_list<TString>(), const TString& description = TString());
    void AddCommand(std::unique_ptr<TClientCommand> command);
    virtual void Config(TConfig& config) override;
    virtual void Parse(TConfig& config) override;
    virtual int Run(TConfig& config) override;
    virtual int Process(TConfig& config) override;
    virtual void RenderCommandsDescription(
        TStringStream& stream,
        const NColorizer::TColors& colors = NColorizer::TColors(false),
        const std::basic_string<bool>& ends = std::basic_string<bool>()
    ) override;
    virtual void SetFreeArgs(TConfig& config);

private:
    bool HasOptionsToShow();
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
