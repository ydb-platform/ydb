#include "client_command_options.h"
#include "profile_manager.h"

#include <ydb/public/lib/ydb_cli/common/common.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/yaml/as/tstring.h>

#include <util/string/strip.h>
#include <util/string/subst.h>
#include <util/system/env.h>

namespace NYdb::NConsoleClient {

TClientCommandOptions::TClientCommandOptions()
    : Opts()
{
}

TClientCommandOptions::TClientCommandOptions(NLastGetopt::TOpts opts)
    : Opts(std::move(opts))
{
}

void TClientCommandOptions::SetTitle(const TString& title) {
    Opts.SetTitle(title);
}

TClientCommandOption& TClientCommandOptions::AddLongOption(const TString& name, const TString& help) {
    return AddClientOption(Opts.AddLongOption(name, help));
}

TClientCommandOption& TClientCommandOptions::AddLongOption(char c, const TString& name, const TString& help) {
    return AddClientOption(Opts.AddLongOption(c, name, help));
}

TClientCommandOption& TClientCommandOptions::AddCharOption(char c, const TString& help) {
    return AddClientOption(Opts.AddCharOption(c, help));
}

TAuthMethodOption& TClientCommandOptions::AddAuthMethodOption(const TString& name, const TString& help) {
    return AddAuthMethodClientOption(Opts.AddLongOption(name, help));
}

void TClientCommandOptions::AddAnonymousAuthMethodOption() {
    ClientOpts.emplace_back(MakeIntrusive<TAnonymousAuthMethodOption>(this));
}

TClientCommandOption& TClientCommandOptions::AddClientOption(NLastGetopt::TOpt& opt) {
    return *ClientOpts.emplace_back(MakeIntrusive<TClientCommandOption>(opt, this));
}

TAuthMethodOption& TClientCommandOptions::AddAuthMethodClientOption(NLastGetopt::TOpt& opt) {
    return static_cast<TAuthMethodOption&>(*ClientOpts.emplace_back(MakeIntrusive<TAuthMethodOption>(opt, this)));
}

void TClientCommandOptions::SetCustomUsage(const TString& usage) {
    Opts.SetCustomUsage(usage);
}

void TClientCommandOptions::SetCmdLineDescr(const TString& descr) {
    Opts.SetCmdLineDescr(descr);
}

void TClientCommandOptions::SetFreeArgsNum(size_t count) {
    Opts.SetFreeArgsNum(count);
}

void TClientCommandOptions::SetFreeArgsNum(size_t min, size_t max) {
    Opts.SetFreeArgsNum(min, max);
}

void TClientCommandOptions::SetFreeArgsMin(size_t min) {
    Opts.SetFreeArgsMin(min);
}

void TClientCommandOptions::SetFreeArgsMax(size_t max) {
    Opts.SetFreeArgsMax(max);
}

void TClientCommandOptions::MutuallyExclusive(TStringBuf opt1, TStringBuf opt2) {
    Opts.MutuallyExclusive(opt1, opt2);
}

void TClientCommandOptions::MutuallyExclusiveOpt(TClientCommandOption& opt1, TClientCommandOption& opt2) {
    Opts.MutuallyExclusiveOpt(opt1.GetOpt(), opt2.GetOpt());
}


TClientCommandOption::TClientCommandOption(NLastGetopt::TOpt& opt, TClientCommandOptions* clientOptions)
    : Opt(&opt)
    , ClientOptions(clientOptions)
    , Help(Opt->GetHelp())
{
}

TClientCommandOption& TClientCommandOption::AdditionalHelpNotes(const TString& helpNotes) {
    HelpNotes = helpNotes;
    RebuildHelpMessage();
    return *this;
}

TClientCommandOption& TClientCommandOption::NoArgument() {
    Opt->NoArgument();
    return *this;
}

TClientCommandOption& TClientCommandOption::RequiredArgument(const TString& title) {
    Opt->RequiredArgument(title);
    return *this;
}

TClientCommandOption& TClientCommandOption::Optional() {
    Opt->Optional();
    return *this;
}

TClientCommandOption& TClientCommandOption::Required() {
    Opt->Required();
    return *this;
}

TClientCommandOption& TClientCommandOption::HasArg(NLastGetopt::EHasArg hasArg) {
    Opt->HasArg(hasArg);
    return *this;
}

TClientCommandOption& TClientCommandOption::Hidden() {
    Opt->Hidden();
    return *this;
}

TClientCommandOption& TClientCommandOption::OptionalArgument(const TString& title) {
    Opt->OptionalArgument(title);
    return *this;
}

TClientCommandOption& TClientCommandOption::AddLongName(const TString& name) {
    Opt->AddLongName(name);
    return *this;
}

TClientCommandOption&  TClientCommandOption::IfPresentDisableCompletion() {
    Opt->IfPresentDisableCompletion();
    return *this;
}

const NLastGetopt::EHasArg& TClientCommandOption::GetHasArg() const {
    return Opt->GetHasArg();
}

TClientCommandOption& TClientCommandOption::StoreResult(TString* result) {
    ValueSetter = [result](const TString& v) {
        *result = v;
    };
    return SetHandler();
}

TClientCommandOption& TClientCommandOption::StoreTrue(bool* target) {
    *target = false;
    Opt->NoArgument();
    ValueSetter = [target](const TString& v) {
        if (!v) {
            *target = true;
        } else if (!TryFromString<bool>(v, *target)) {
            *target = false;
        }
    };
    return SetHandler();
}

TClientCommandOption& TClientCommandOption::StoreFilePath(TString* filePath) {
    FilePath = filePath;
    return *this;
}

TClientCommandOption& TClientCommandOption::Handler(THandler handler) {
    ValueHandler = std::move(handler);
    return SetHandler();
}

TClientCommandOption& TClientCommandOption::Validator(TValidator validator) {
    ValidatorHandler = std::move(validator);
    return SetHandler();
}

TClientCommandOption& TClientCommandOption::Handler(void (*handler)(const NLastGetopt::TOptsParser*)) {
    Opt->Handler(handler);
    return *this;
}

TClientCommandOption& TClientCommandOption::SetHandler() {
    if (!HandlerIsSet) {
        struct THandler : public NLastGetopt::IOptHandler {
            THandler(TClientCommandOption* self)
                : Self(self)
            {
            }

            void HandleOpt(const NLastGetopt::TOptsParser* parser) override {
                TString curVal(parser->CurValOrDef());
                Self->HandlerImpl(std::move(curVal), Self->IsFileName, Self->HumanReadableFileName, EOptionValueSource::Explicit);
            }

            TClientCommandOption* Self;
        };
        Opt->Handler(new THandler(this));
        HandlerIsSet = true;
    }
    return *this;
}

bool TClientCommandOption::HandlerImpl(TString value, bool isFileName, const TString& humanReadableFileName, EOptionValueSource valueSource) {
    if (isFileName) {
        TString path = value;
        if (valueSource == EOptionValueSource::DefaultValue && !ReadFromFileIfExists(path, humanReadableFileName ? humanReadableFileName : path, value)) {
            return false;
        }
        value = ReadFromFile(path, humanReadableFileName ? humanReadableFileName : path);
        if (FilePath) {
            *FilePath = std::move(path);
        }
    }
    if (ValueHandler) {
        ValueHandler(value);
    }
    if (ValueSetter) {
        ValueSetter(value);
    }
    return true;
}

TClientCommandOption& TClientCommandOption::FileName(const TString& humanReadableFileName, bool fileName) {
    HumanReadableFileName = humanReadableFileName;
    IsFileName = fileName;
    return *this;
}

TClientCommandOption& TClientCommandOption::Env(const TString& envName, bool isFileName, const TString& humanReadableFileName) {
    EnvInfo.emplace_back(
        TEnvInfo{
            .EnvName = envName,
            .IsFileName = isFileName,
            .HumanReadableFileName = humanReadableFileName,
        }
    );
    RebuildHelpMessage();
    return *this;
}

TClientCommandOption& TClientCommandOption::ProfileParam(const TString& profileParamName, bool isFileName) {
    ProfileParamName = profileParamName;
    ProfileParamIsFileName = isFileName;
    CanParseFromProfile = true;
    RebuildHelpMessage();
    return *this;
}

TClientCommandOption& TClientCommandOption::SetSupportsProfile(bool supports) {
    CanParseFromProfile = supports;
    RebuildHelpMessage();
    return *this;
}

TClientCommandOption& TClientCommandOption::LogToConnectionParams(const TString& paramName) {
    ConnectionParamName = paramName;
    return *this;
}

TClientCommandOption& TClientCommandOption::DocLink(const TString& link) {
    Documentation = link;
    RebuildHelpMessage();
    return *this;
}

TClientCommandOption& TClientCommandOption::DefaultValue(const TString& defaultValue) {
    DefaultOptionValue = defaultValue;
    RebuildHelpMessage();
    return *this;
}

TClientCommandOption& TClientCommandOption::ManualDefaultValueDescription(const TString& description) {
    ManualDefaultOptionValueDescription = description;
    RebuildHelpMessage();
    return *this;
}

bool TClientCommandOption::NeedPrintDefinitionsPriority() const {
    return CanParseFromProfile || !EnvInfo.empty(); // If only this option and default value => no need to print priority
}

void TClientCommandOption::RebuildHelpMessage() {
    NColorizer::TColors& colors = NColorizer::AutoColors(Cout);
    TStringBuilder helpMessage;
    helpMessage << Help;
    const bool needDefinitionsPriority = ClientOptions->HelpCommandVerbosiltyLevel >= 2 && NeedPrintDefinitionsPriority();

    if (!needDefinitionsPriority && (DefaultOptionValue || ManualDefaultOptionValueDescription)) {
        helpMessage << " (default: " << colors.Cyan()
            << (DefaultOptionValue ? DefaultOptionValue : ManualDefaultOptionValueDescription)
            << colors.OldColor() << ")";
    }

    bool multiline = false;
    auto makeMultiline = [&]() {
        if (multiline) {
            return;
        }
        multiline = true;
        if (!Help.EndsWith('.')) {
            helpMessage << ".";
        }
        helpMessage << Endl;
    };

    TString indent = ClientOptions->HelpCommandVerbosiltyLevel >= 2 ? "  " : "";
    if (Documentation) {
        makeMultiline();
        helpMessage << indent << "For more info go to: " << Documentation << Endl;
    }
    if (needDefinitionsPriority) {
        makeMultiline();
        helpMessage << indent << "Definition priority:";
        size_t currentPoint = 1;
        helpMessage << Endl << indent << indent << currentPoint++ << ". This option";
        if (CanParseFromProfile) {
            helpMessage << Endl << indent << indent << currentPoint++ << ". Profile specified with --profile option";
        }
        for (const TEnvInfo& envInfo : EnvInfo) {
            helpMessage << Endl << indent << indent << currentPoint++ << ". "
                << colors.BoldColor() << envInfo.EnvName << colors.OldColor()
                << " environment variable";
        }
        if (CanParseFromProfile) {
            helpMessage << Endl << indent << indent << currentPoint++ << ". Active configuration profile";
        }
        if (DefaultOptionValue || ManualDefaultOptionValueDescription) {
            if (DefaultOptionValue) {
                helpMessage << Endl << indent << indent << currentPoint++ << ". Default value: " << colors.Cyan() << DefaultOptionValue << colors.OldColor();
            } else {
                helpMessage << Endl << indent << indent << currentPoint++ << ". " << ManualDefaultOptionValueDescription;
            }
        }
    } else {
        if (!EnvInfo.empty()) {
            makeMultiline();
            helpMessage << indent << "Environment variable" << (EnvInfo.size() > 1 ? "s: " : ": ");
            for (size_t i = 0; i < EnvInfo.size(); ++i) {
                if (i) {
                    helpMessage << ", ";
                }
                helpMessage << colors.BoldColor() << EnvInfo[i].EnvName << colors.OldColor();
            }
        }
    }
    if (HelpNotes) {
        TString notes = Strip(HelpNotes);
        SubstGlobal(notes, "\n", TStringBuilder() << "\n" << indent);
        helpMessage << Endl << Endl;
        helpMessage << indent << notes;
    }
    Opt->Help(helpMessage);
}

bool TClientCommandOption::TryParseFromProfile(const std::shared_ptr<IProfile>& profile, TString* parsedValue, bool* isFileName, std::vector<TString>* errors, bool parseOnly) const {
    Y_UNUSED(errors, parseOnly);
    if (profile && ProfileParamName && profile->Has(ProfileParamName)) {
        TString value = profile->GetValue(ProfileParamName).as<TString>();
        if (parsedValue) {
            *parsedValue = value;
        }
        if (isFileName) {
            *isFileName = ProfileParamIsFileName;
        }
        return true;
    }
    return false;
}


TAuthMethodOption::TAuthMethodOption(NLastGetopt::TOpt& opt, TClientCommandOptions* clientOptions)
    : TClientCommandOption(opt, clientOptions)
{
}

TAuthMethodOption& TAuthMethodOption::AuthMethod(const TString& methodName) {
    AuthMethodName = methodName;
    return *this;
}

TAuthMethodOption& TAuthMethodOption::AuthProfileParser(TProfileParser parser, const TString& authMethod) {
    CanParseFromProfile = true;
    ProfileParsers[authMethod ? authMethod : AuthMethodName] = std::move(parser);
    return *this;
}

TAuthMethodOption& TAuthMethodOption::SimpleProfileDataParam(const TString& authMethod, bool isFileName) {
    auto parser = [this, isFileName, authMethod](const YAML::Node& authData, TString* value, bool* isFileNameOut, std::vector<TString>* errors, bool parseOnly) -> bool {
        Y_UNUSED(errors, parseOnly);

        const bool needData = GetHasArg() != NLastGetopt::NO_ARGUMENT;
        if (!needData) {
            if (value) {
                *value = {};
            }
            return true;
        }

        if (value) {
            *value = authData.as<TString>();
        }
        if (isFileNameOut) {
            *isFileNameOut = isFileName;
        }
        return true;
    };
    return AuthProfileParser(std::move(parser), authMethod);
}

bool TAuthMethodOption::TryParseFromProfile(const std::shared_ptr<IProfile>& profile, TString* parsedValue, bool* isFileName, std::vector<TString>* errors, bool parseOnly) const {
    if (!profile || !profile->Has("authentication")) {
        return false;
    }

    YAML::Node authValue = profile->GetValue("authentication");
    YAML::Node methodValue = authValue["method"];
    if (!methodValue) {
        if (errors) {
            errors->push_back("Configuration profile has \"authentication\" but does not have \"method\" in it");
        }
        return false;
    }

    TString authMethod = methodValue.as<TString>();
    const auto parser = ProfileParsers.find(authMethod);
    if (parser == ProfileParsers.end()) {
        return false;
    }
    return parser->second(authValue["data"], parsedValue, isFileName, errors, parseOnly);
}


TAnonymousAuthMethodOption::TAnonymousAuthMethodOption(TClientCommandOptions* clientOptions)
    : TAuthMethodOption(*this, clientOptions)
{
    TOpt::NoArgument();
    AuthMethod("anonymous-auth");
    SimpleProfileDataParam();
}


TOptionsParseResult::TOptionsParseResult(const TClientCommandOptions* options, int argc, const char** argv)
    : ClientOptions(options)
    , ParseFromCommandLineResult(&options->GetOpts(), argc, argv)
{
    for (const auto& clientOption : ClientOptions->ClientOpts) {
        if (const auto* optResult = ParseFromCommandLineResult.FindOptParseResult(&clientOption->GetOpt())) {
            Opts.emplace_back(clientOption, optResult);
            if (dynamic_cast<const TAuthMethodOption*>(clientOption.Get())) {
                AuthMethodOpts.push_back(Opts.size() - 1); // insert index
            }
        }
    }
}

const TOptionParseResult* TOptionsParseResult::FindResult(const TClientCommandOption* opt) const {
    for (const TOptionParseResult& result : Opts) {
        if (result.GetOpt() == opt) {
            return &result;
        }
    }
    return nullptr;
}

const TOptionParseResult* TOptionsParseResult::FindResult(const TString& name) const {
    for (const auto& opt : Opts) {
        if (IsIn(opt.GetOpt()->GetOpt().GetLongNames(), name)) {
            return &opt;
        }
    }
    return nullptr;
}

const TOptionParseResult* TOptionsParseResult::FindResult(char name) const {
    for (const auto& opt : Opts) {
        if (IsIn(opt.GetOpt()->GetOpt().GetShortNames(), name)) {
            return &opt;
        }
    }
    return nullptr;
}

size_t TOptionsParseResult::GetFreeArgsPos() const {
    return ParseFromCommandLineResult.GetFreeArgsPos();
}

size_t TOptionsParseResult::GetFreeArgCount() const {
    return ParseFromCommandLineResult.GetFreeArgCount();
}

TVector<TString> TOptionsParseResult::GetFreeArgs() const {
    return ParseFromCommandLineResult.GetFreeArgs();
}

bool TOptionsParseResult::Has(const TString& name, bool includeDefault) const {
    const TOptionParseResult* result = FindResult(name);
    if (!includeDefault && result && result->ValueSource == EOptionValueSource::DefaultValue) {
        result = nullptr;
    }
    return result != nullptr;
}

bool TOptionsParseResult::Has(char name, bool includeDefault) const {
    const TOptionParseResult* result = FindResult(name);
    if (!includeDefault && result && result->ValueSource == EOptionValueSource::DefaultValue) {
        result = nullptr;
    }
    return result != nullptr;
}

const TString& TOptionsParseResult::Get(const TString& name, bool includeDefault) const {
    if (const TOptionParseResult* result = FindResult(name); result && (includeDefault || result->ValueSource != EOptionValueSource::DefaultValue)) {
        return result->Values().back();
    }
    throw yexception() << "No \"" << name << "\" option";
}

std::vector<TString> TOptionsParseResult::LogConnectionParams(const TConnectionParamsLogger& logger) {
    std::vector<TString> messages;
    auto getProfileOpt = [&](const TIntrusivePtr<TClientCommandOption>& opt, const std::shared_ptr<IProfile>& profile) -> TString {
        TString value;
        if (opt->TryParseFromProfile(profile, &value, nullptr, &messages, true)) {
            return value;
        }
        return {};
    };
    auto processValue = [&](const TIntrusivePtr<TClientCommandOption>& opt, const TString& value, const TString& sourceDescription, bool validate) {
        if (validate && opt->ValidatorHandler) {
            if (std::vector<TString> msgs = opt->ValidatorHandler(value); !msgs.empty()) {
                messages.insert(messages.end(), msgs.begin(), msgs.end());
            }
        }
        logger(opt->ConnectionParamName, value, sourceDescription);
    };
    for (const TOptionParseResult& result : Opts) {
        const TIntrusivePtr<TClientCommandOption>& opt = result.Opt;
        if (opt->ConnectionParamName) {
            // Log all available sources for current option
            for (EOptionValueSource src = result.ValueSource; ; src = static_cast<EOptionValueSource>(static_cast<int>(src) + 1)) {
                const bool validate = src != result.ValueSource; // We validate result in Validate() method
                switch (src) {
                case EOptionValueSource::Explicit: {
                    // already have parsed in that source
                    TStringBuilder txt;
                    txt << "explicit";
                    if (auto name = opt->GetOpt().GetLongNames()) {
                        txt << " --" << name[0];
                    } else if (auto shortNames = opt->GetOpt().GetShortNames()) {
                        txt << " -" << shortNames[0];
                    }
                    txt << " option";
                    for (const TString& value : result.OptValues) {
                        processValue(opt, value, txt, validate);
                    }
                    break;
                }
                case EOptionValueSource::ExplicitProfile: {
                    if (TString value = getProfileOpt(opt, ExplicitProfile)) {
                        TStringBuilder txt;
                        txt << "profile \"" << ExplicitProfile->GetName() << "\" from explicit --profile option";
                        processValue(opt, value, txt, validate);
                    }
                    break;
                }
                case EOptionValueSource::ActiveProfile: {
                    if (TString value = getProfileOpt(opt, ActiveProfile)) {
                        TStringBuilder txt;
                        txt << "active profile \"" << ActiveProfile->GetName() << "\"";
                        processValue(opt, value, txt, validate);
                    }
                    break;
                }
                case EOptionValueSource::EnvironmentVariable: {
                    for (const auto& envInfo : opt->EnvInfo) {
                        if (TString value = GetEnv(envInfo.EnvName)) {
                            TStringBuilder txt;
                            txt << envInfo.EnvName << " enviroment variable";
                            processValue(opt, value, txt, validate);
                        }
                    }
                    break;
                }
                case EOptionValueSource::DefaultValue:
                    if (opt->DefaultOptionValue) {
                        processValue(opt, opt->DefaultOptionValue, "default value", validate);
                    }
                    break;
                }
                if (src == EOptionValueSource::DefaultValue) {
                    break;
                }
            }
        }
    }

    // Log chosen auth method
    if (AuthMethodOpts.size() == 1) {
        const auto& opt = Opts[AuthMethodOpts[0]];
        Cerr << "Using auth method \"" << ChosenAuthMethod << "\"";
        switch (opt.ValueSource) {
        case EOptionValueSource::Explicit:
            Cerr << " from explicit command line option ";
            if (auto name = opt.Opt->GetOpt().GetLongNames()) {
                Cerr << "--" << name[0];
            } else if (auto shortNames = opt.Opt->GetOpt().GetShortNames()) {
                Cerr << "-" << shortNames[0];
            }
            break;
        case EOptionValueSource::ExplicitProfile:
            Cerr << " from explicitly specified profile \"" << ExplicitProfile->GetName() << "\"";
            break;
        case EOptionValueSource::ActiveProfile:
            Cerr << " from current active profile \"" << ActiveProfile->GetName() << "\"";
            break;
        case EOptionValueSource::EnvironmentVariable:
            for (const auto& envInfo : opt.Opt->EnvInfo) {
                if (TString value = GetEnv(envInfo.EnvName)) {
                    TStringBuilder txt;
                    Cerr << " from " << envInfo.EnvName << " enviroment variable";
                    break;
                }
            }
            break;
        case EOptionValueSource::DefaultValue:
            Cerr << " from default value \"" << opt.Opt->DefaultOptionValue << "\"";
            break;
        }
        Cerr << Endl;
    } else {
        Cerr << "No authentication methods were found. Going without authentication" << Endl;
    }

    return messages;
}

std::vector<TString> TOptionsParseResult::ParseFromProfilesAndEnv(std::shared_ptr<IProfile> explicitProfile, std::shared_ptr<IProfile> activeProfile) {
    ExplicitProfile = std::move(explicitProfile);
    ActiveProfile = std::move(activeProfile);
    std::vector<TString> errors;

    auto applyOption = [&](const TIntrusivePtr<TClientCommandOption>& clientOption, const TString& value, bool isFileName, const TString& humanReadableFileName, EOptionValueSource valueSource) {
        if (clientOption->HandlerImpl(value, isFileName, humanReadableFileName, valueSource)) { // returns false only when loading from default value from file
            Opts.emplace_back(clientOption, value, valueSource);
            if (dynamic_cast<const TAuthMethodOption*>(clientOption.Get())) {
                AuthMethodOpts.push_back(Opts.size() - 1);
            }
            if (clientOption->ValidatorHandler) {
                if (std::vector<TString> msgs = clientOption->ValidatorHandler(value); !msgs.empty()) {
                    errors.insert(errors.end(), msgs.begin(), msgs.end());
                }
            }
        }
    };

    for (const TIntrusivePtr<TClientCommandOption>& clientOption : ClientOptions->ClientOpts) {
        if (FindResult(clientOption.Get())) {
            continue;
        }
        const bool isAuthOption = dynamic_cast<const TAuthMethodOption*>(clientOption.Get()) != nullptr;
        if (isAuthOption && !AuthMethodOpts.empty()) { // Parsed from command line or from profile
            continue;
        }

        bool isFileName = false;
        if (TString value; clientOption->TryParseFromProfile(ExplicitProfile, &value, &isFileName, &errors, false)) {
            applyOption(clientOption, value, isFileName, clientOption->HumanReadableFileName, EOptionValueSource::ExplicitProfile);
            continue;
        }
        if (isAuthOption) {
            continue;
        }

        bool envApplied = false;
        for (const auto& envInfo : clientOption->EnvInfo) {
            if (TString value = GetEnv(envInfo.EnvName)) {
                applyOption(clientOption, value, envInfo.IsFileName, envInfo.HumanReadableFileName, EOptionValueSource::EnvironmentVariable);
                envApplied = true;
                break;
            }
        }
        if (envApplied) {
            continue;
        }

        if (ActiveProfile != ExplicitProfile) {
            if (TString value; clientOption->TryParseFromProfile(ActiveProfile, &value, &isFileName, &errors, false)) {
                applyOption(clientOption, value, isFileName, clientOption->HumanReadableFileName, EOptionValueSource::ActiveProfile);
                continue;
            }
        }

        if (clientOption->DefaultOptionValue) {
            applyOption(clientOption, clientOption->DefaultOptionValue, clientOption->IsFileName, clientOption->HumanReadableFileName, EOptionValueSource::DefaultValue);
        }
    }

    if (AuthMethodOpts.empty()) { // have not parsed from command line and from explicit profile. Try from env
        for (const TIntrusivePtr<TAuthMethodOption>& clientOption : ClientOptions->EnvAuthPriority) {
            for (const auto& envInfo : clientOption->EnvInfo) {
                if (TString value = GetEnv(envInfo.EnvName)) {
                    applyOption(clientOption, value, envInfo.IsFileName, envInfo.HumanReadableFileName, EOptionValueSource::EnvironmentVariable);
                    break;
                }
            }
            if (!AuthMethodOpts.empty()) {
                break;
            }
        }
    }

    if (AuthMethodOpts.empty()) { // try active profile
        for (const TIntrusivePtr<TClientCommandOption>& clientOption : ClientOptions->ClientOpts) {
            if (!dynamic_cast<const TAuthMethodOption*>(clientOption.Get())) {
                continue;
            }

            if (ActiveProfile != ExplicitProfile) {
                bool isFileName = false;
                if (TString value; clientOption->TryParseFromProfile(ActiveProfile, &value, &isFileName, &errors, false)) {
                    applyOption(clientOption, value, isFileName, clientOption->HumanReadableFileName, EOptionValueSource::ActiveProfile);
                    continue;
                }
            }

            if (!AuthMethodOpts.empty()) {
                break;
            }
        }
    }

    if (AuthMethodOpts.empty()) { // try default
        for (const TIntrusivePtr<TClientCommandOption>& clientOption : ClientOptions->ClientOpts) {
            if (!dynamic_cast<const TAuthMethodOption*>(clientOption.Get())) {
                continue;
            }

            if (clientOption->DefaultOptionValue) {
                applyOption(clientOption, clientOption->DefaultOptionValue, clientOption->IsFileName, clientOption->HumanReadableFileName, EOptionValueSource::DefaultValue);
            }

            if (!AuthMethodOpts.empty()) {
                break;
            }
        }
    }

    if (AuthMethodOpts.size() == 1) {
        ChosenAuthMethod = static_cast<const TAuthMethodOption*>(Opts[AuthMethodOpts[0]].Opt.Get())->GetAuthMethod();
    }
    return errors;
}

std::vector<TString> TOptionsParseResult::Validate() {
    std::vector<TString> messages;
    if (AuthMethodOpts.size() > 1) {
        TStringBuilder err;
        err << AuthMethodOpts.size() << " methods were provided via options:";
        bool comma = false;
        for (size_t method : AuthMethodOpts) {
            if (comma) {
                err << ",";
            }
            comma = true;
            const TOptionParseResult& opt = Opts[method];
            if (auto names = opt.Opt->GetOpt().GetLongNames()) {
                err << " --" << names[0];
            } else if (auto shortNames = opt.Opt->GetOpt().GetShortNames()) {
                err << " -" << shortNames[0];
            }
        }
        err << ". Choose exactly one of them";
        messages.push_back(err);
    }
    for (const TOptionParseResult& result : Opts) {
        if (result.Opt->ValidatorHandler) {
            for (const TString& value : result.Values()) {
                if (std::vector<TString> msgs = result.Opt->ValidatorHandler(value); !msgs.empty()) {
                    messages.insert(messages.end(), msgs.begin(), msgs.end());
                }
            }
        }
    }
    return messages;
}

} // namespace NYdb::NConsoleClient
