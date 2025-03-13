#include "command.h"
#include "command_utils.h"
#include "normalize_path.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb {
namespace NConsoleClient {

bool TClientCommand::TIME_REQUESTS = false; // measure time of requests
bool TClientCommand::PROGRESS_REQUESTS = false; // display progress of long requests

using namespace NUtils;

TClientCommand::TClientCommand(
    const TString& name,
    const std::initializer_list<TString>& aliases,
    const TString& description,
    bool visible)
        : Name(name)
        , Aliases(aliases)
        , Description(description)
        , Visible(visible)
        , Parent(nullptr)
        , Opts(NLastGetopt::TOpts::Default())
{
    HideOption("svnrevision");
    Opts.AddHelpOption('h');
    ChangeOptionDescription("help", "Print usage, -hh for detailed help");
    auto terminalWidth = GetTerminalWidth();
    size_t lineLength = terminalWidth ? *terminalWidth : Max<size_t>();
    Opts.SetWrap(Max(Opts.Wrap_, static_cast<ui32>(lineLength)));
}

ELogPriority TClientCommand::TConfig::VerbosityLevelToELogPriority(TClientCommand::TConfig::EVerbosityLevel lvl) {
    switch (lvl) {
        case TClientCommand::TConfig::EVerbosityLevel::NONE:
            return ELogPriority::TLOG_EMERG;
        case TClientCommand::TConfig::EVerbosityLevel::DEBUG:
            return ELogPriority::TLOG_DEBUG;
        case TClientCommand::TConfig::EVerbosityLevel::INFO:
            return ELogPriority::TLOG_INFO;
        case TClientCommand::TConfig::EVerbosityLevel::WARN:
            return ELogPriority::TLOG_WARNING;
        default:
            return ELogPriority::TLOG_ERR;
    }
}

ELogPriority TClientCommand::TConfig::VerbosityLevelToELogPriorityChatty(TClientCommand::TConfig::EVerbosityLevel lvl) {
    switch (lvl) {
        case TClientCommand::TConfig::EVerbosityLevel::NONE:
            return ELogPriority::TLOG_INFO;
        case TClientCommand::TConfig::EVerbosityLevel::DEBUG:
        case TClientCommand::TConfig::EVerbosityLevel::INFO:
        case TClientCommand::TConfig::EVerbosityLevel::WARN:
            return ELogPriority::TLOG_DEBUG;
    }
    return ELogPriority::TLOG_INFO;
}

size_t TClientCommand::TConfig::ParseHelpCommandVerbosilty(int argc, char** argv) {
    size_t cnt = 0;
    for (int i = 0; i < argc; ++i) {
        TStringBuf arg = argv[i];
        if (arg == "--help") {
            ++cnt;
            continue;
        }
        if (arg.StartsWith("--")) { // other option
            continue;
        }
        if (arg.StartsWith("-")) { // char options
            for (size_t i = 1; i < arg.size(); ++i) {
                if (arg[i] == 'h') {
                    ++cnt;
                }
            }
        }
    }

    if (!cnt) {
        cnt = 1;
    }
    return cnt;
}

namespace {
    class TSingleProviderFactory : public ICredentialsProviderFactory {
    public:
        TSingleProviderFactory(std::shared_ptr<ICredentialsProviderFactory> originalFactory)
        : OriginalFactory(originalFactory)
        {}
    virtual std::shared_ptr<ICredentialsProvider> CreateProvider() const override {
        if (!provider) {
            provider = OriginalFactory->CreateProvider();
        }
        return provider;
    }
    virtual std::shared_ptr<ICredentialsProvider> CreateProvider(std::weak_ptr<ICoreFacility> facility) const override {
        if (!provider) {
            provider = OriginalFactory->CreateProvider(facility);
        }
        return provider;
    }

    private:
        std::shared_ptr<ICredentialsProviderFactory> OriginalFactory;
        mutable TCredentialsProviderPtr provider = nullptr;
    };
}

std::shared_ptr<ICredentialsProviderFactory> TClientCommand::TConfig::GetSingletonCredentialsProviderFactory() {
    if (!SingletonCredentialsProviderFactory) {
        auto credentialsGetterResult = CredentialsGetter(*this);
        if (credentialsGetterResult) {
            SingletonCredentialsProviderFactory = std::make_shared<TSingleProviderFactory>(credentialsGetterResult);
        }
    }
    return SingletonCredentialsProviderFactory;
}

TClientCommand::TOptsParseOneLevelResult::TOptsParseOneLevelResult(TConfig& config) {
    int _argc = 1;
    int levels = 1;

    while (levels > 0 && _argc < config.ArgC) {
        if (config.ArgV[_argc][0] == '-') {
            const NLastGetopt::TOpt* opt = nullptr;
            TStringBuf optName(config.ArgV[_argc]);
            auto eqPos = optName.find('=');
            optName = optName.substr(0, eqPos);
            if (optName.StartsWith("--")) {
                opt = config.Opts->FindLongOption(optName.substr(2));
                if (opt != nullptr && opt->GetHasArg() != NLastGetopt::NO_ARGUMENT && eqPos == TStringBuf::npos) {
                    ++_argc;
                }
            } else {
                if (optName.length() > 2) {
                    // Char option list
                    if (eqPos != TStringBuf::npos) {
                        throw yexception() << "Char option list " << optName << " can not be followed by \"=\" sign";
                    }
                } else if (optName.length() == 2) {
                    // Single char option
                    opt = config.Opts->FindCharOption(optName[1]);
                    if (opt != nullptr && opt->GetHasArg() != NLastGetopt::NO_ARGUMENT && eqPos == TStringBuf::npos) {
                        ++_argc;
                    }
                } else {
                    throw yexception() << "Wrong CLI argument \"" << optName << "\"";
                }
            }
        } else {
            --levels;
        }
        ++_argc;
    }
    Init(config.Opts, _argc, const_cast<const char**>(config.ArgV));
}

void TClientCommand::CheckForExecutableOptions(TConfig& config) {
    int argc = 1;

    while (argc < config.ArgC && config.ArgV[argc][0] == '-') {
        const NLastGetopt::TOpt* opt = nullptr;
        TStringBuf optName(config.ArgV[argc]);
        auto eqPos = optName.find('=');
        optName = optName.substr(0, eqPos);
        if (optName.StartsWith("--")) {
            opt = config.Opts->FindLongOption(optName.substr(2));
        } else {
            if (optName.length() > 2) {
                // Char option list
                if (eqPos != TStringBuf::npos) {
                    throw yexception() << "Char option list " << optName << " can not be followed by \"=\" sign";
                }
            } else if (optName.length() == 2) {
                // Single char option
                opt = config.Opts->FindCharOption(optName[1]);
            } else {
                throw yexception() << "Wrong CLI argument \"" << optName << "\"";
            }
        }
        if (config.ExecutableOptions.find(optName) != config.ExecutableOptions.end()) {
            config.HasExecutableOptions = true;
        }
        if (opt != nullptr && opt->GetHasArg() != NLastGetopt::NO_ARGUMENT) {
            if (eqPos == TStringBuf::npos) {
                ++argc;
            }
        }
        ++argc;
    }
}

void TClientCommand::Config(TConfig& config) {
    config.Opts = &Opts;
    config.OnlyExplicitProfile = OnlyExplicitProfile;
    TStringStream stream;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    stream << Endl << Endl
        << colors.BoldColor() << "Description" << colors.OldColor() << ": " << Description << Endl << Endl;
    PrintParentOptions(stream, config, colors);
    config.Opts->SetCmdLineDescr(stream.Str());
}

void TClientCommand::Parse(TConfig& config) {
    Y_UNUSED(config);
}

void TClientCommand::Validate(TConfig& config) {
    Y_UNUSED(config);
}

int TClientCommand::Run(TConfig& config) {
    Y_UNUSED(config);
    // TODO: invalid usage ? error? help?
    return EXIT_FAILURE;
}

int TClientCommand::Process(TConfig& config) {
    Prepare(config);
    return ValidateAndRun(config);
}

void TClientCommand::SaveParseResult(TConfig& config) {
    ParseResult = std::make_shared<NLastGetopt::TOptsParseResult>(config.Opts, config.ArgC, config.ArgV);
}

void TClientCommand::Prepare(TConfig& config) {
    config.ArgsSettings = TConfig::TArgSettings();
    config.Opts = &Opts;
    Config(config);
    CheckForExecutableOptions(config);
    config.CheckParamsCount();
    SetCustomUsage(config);
    SaveParseResult(config);
    config.ParseResult = ParseResult.get();
    Parse(config);
}

void TClientCommand::ExtractParams(TConfig& config) {
    Y_UNUSED(config);
}

bool TClientCommand::Prompt(TConfig& config) {
    Y_UNUSED(config);
    return true;
}

int TClientCommand::ValidateAndRun(TConfig& config) {
    config.Opts = &Opts;
    config.ParseResult = ParseResult.get();
    ExtractParams(config);
    Validate(config);
    if (Prompt(config)) {
        return Run(config);
    } else {
        return EXIT_FAILURE;
    }
}

void TClientCommand::SetCustomUsage(TConfig& config) {
    // command1 [global options...] command2 ... lastCommand [options...]
    TStringBuilder fullName;
    bool foundRootParent = false;
    for (const auto& parent : config.ParentCommands) {
        fullName << parent.Name;
        if (parent.Options) {
            if (!foundRootParent) {
                foundRootParent = true;
                fullName << " [global options...]";
            } else {
                fullName << " [" << parent.Name << " options...]";
            }
        }
        fullName << " ";
    }
    fullName << config.ArgV[0] << " [options...]";
    for (auto& arg : Args) {
        fullName << ' ' << arg.second;
    }
    config.Opts->SetCustomUsage(fullName);
}

void TClientCommand::HideOption(const TString& name) {
    NLastGetopt::TOpt* opt = Opts.FindLongOption(name);
    if (opt) {
        opt->Hidden_ = true;
    }
}

void TClientCommand::ChangeOptionDescription(const TString& name, const TString& description) {
    NLastGetopt::TOpt* opt = Opts.FindLongOption(name);
    if (opt) {
        opt->Help_ = description;
    }
}

void TClientCommand::SetFreeArgTitle(size_t pos, const TString& title, const TString& help) {
    Args[pos] = title;
    Opts.SetFreeArgTitle(pos, title, help);
}

void TClientCommand::RenderOneCommandDescription(
    TStringStream& stream,
    const NColorizer::TColors& colors,
    RenderEntryType type
) {
    if (Hidden && type != BEGIN) {
        return;
    }
    TString prefix;
    if (type == MIDDLE) {
        prefix = "├─ ";
    }
    if (type == END) {
        prefix = "└─ ";
    }
    TString line = prefix + Name;
    stream << prefix << (Dangerous ? colors.Red() : "") << colors.BoldColor() << Name << colors.OldColor();
    if (!Description.empty()) {
        int namePartLength = GetNumberOfUTF8Chars(line);
        if (namePartLength < DESCRIPTION_ALIGNMENT)
            stream << TString(DESCRIPTION_ALIGNMENT - namePartLength, ' ');
        else
            stream << ' ';
        stream << Description;
        if (!Aliases.empty()) {
            stream << " (aliases: ";
            for (auto it = Aliases.begin(); it != Aliases.end(); ++it) {
                if (it != Aliases.begin())
                    stream << ", ";
                stream << colors.BoldColor() << *it << colors.OldColor();
            }
            stream << ')';
        }
    }
    stream << '\n';
}

void TClientCommand::Hide() {
    Hidden = true;
    Visible = false;
}

void TClientCommand::MarkDangerous() {
    Dangerous = true;
}

void TClientCommand::UseOnlyExplicitProfile() {
    OnlyExplicitProfile = true;
}

TClientCommandTree::TClientCommandTree(const TString& name, const std::initializer_list<TString>& aliases, const TString& description)
    : TClientCommand(name, aliases, description)
    , SelectedCommand(nullptr)
{
    Args[0] = "<subcommand>";
}

void TClientCommandTree::AddCommand(std::unique_ptr<TClientCommand> command) {
    for (const TString& alias : command->Aliases) {
        Aliases[alias] = command->Name;
    }
    command->Parent = this;
    SubCommands[command->Name] = std::move(command);
}

void TClientCommandTree::AddHiddenCommand(std::unique_ptr<TClientCommand> command) {
    command->Hide();
    AddCommand(std::move(command));
}

void TClientCommandTree::AddDangerousCommand(std::unique_ptr<TClientCommand> command) {
    command->MarkDangerous();
    command->UseOnlyExplicitProfile();
    AddCommand(std::move(command));
}

void TClientCommandTree::Config(TConfig& config) {
    TClientCommand::Config(config);
    SetFreeArgs(config);
    TString commands;
    SetFreeArgTitle(0, "<subcommand>", commands);
    TStringStream stream;
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    stream << Endl << Endl
        << colors.BoldColor() << "Description" << colors.OldColor() << ": " << Description << Endl << Endl
        << colors.BoldColor() << "Subcommands" << colors.OldColor() << ":" << Endl;
    RenderCommandsDescription(stream, colors);
    stream << Endl;
    PrintParentOptions(stream, config, colors);
    config.Opts->SetCmdLineDescr(stream.Str());
}

void TClientCommandTree::SaveParseResult(TConfig& config) {
    ParseResult = std::make_shared<TOptsParseOneLevelResult>(config);
}

void TClientCommandTree::Parse(TConfig& config) {
    TClientCommand::Parse(config);

    if (config.ParseResult->GetFreeArgs().empty()) {
        return;
    }

    TString cmd = config.ParseResult->GetFreeArgs().at(0);
    config.Tokens.push_back(cmd);
    size_t count = config.ParseResult->GetFreeArgsPos();
    config.ArgC -= count;
    config.ArgV += count;
    {
        auto it = Aliases.find(cmd);
        if (it != Aliases.end())
            cmd = it->second;
    }
    auto it = SubCommands.find(cmd);
    if (it == SubCommands.end()) {
        if (IsNumber(cmd))
            it = SubCommands.find("#");
        if (it == SubCommands.end())
            it = SubCommands.find("*");
    }
    if (it != SubCommands.end()) {
        SelectedCommand = it->second.get();
    } else {
        throw yexception() << "Invalid command '" << cmd << "'";
    }
}

int TClientCommandTree::Run(TConfig& config) {
    if (SelectedCommand) {
        return SelectedCommand->ValidateAndRun(config);
    }
    throw yexception() << "No child command to run";
}

void TClientCommandTree::Prepare(TConfig& config) {
    TClientCommand::Prepare(config);
    config.ParentCommands.push_back({ Name, HasOptionsToShow() ? &Opts : nullptr });

    if (SelectedCommand) {
        SelectedCommand->Prepare(config);
    }
}

void TClientCommandTree::SetFreeArgs(TConfig& config) {
    config.SetFreeArgsMin(1);
}

bool TClientCommandTree::HasOptionsToShow() {
    for (auto opt : Opts.Opts_) {
        if (!NeedToHideOption(opt.Get())) {
            return true;
        }
    }
    return false;
}

void TClientCommandTree::RenderCommandsDescription(
    TStringStream& stream,
    const NColorizer::TColors& colors
) {
    TClientCommand::RenderOneCommandDescription(stream, colors, BEGIN);

    TVector<TClientCommand*> VisibleSubCommands;
    for (auto& [_, command] : SubCommands) {
        if (command->Visible) {
            VisibleSubCommands.push_back(command.get());
        }
    }

    for (auto it = VisibleSubCommands.begin(); it != VisibleSubCommands.end(); ++it) {
        bool lastCommand = (std::next(it) == VisibleSubCommands.end());
        (*it)->RenderOneCommandDescription(stream, colors, lastCommand ? END : MIDDLE);
    }
}

void TCommandWithPath::ParsePath(const TClientCommand::TConfig& config, const size_t argPos, bool isPathOptional) {
    if (config.ParseResult->GetFreeArgCount() <= argPos) {
        if (isPathOptional) {
            Path = ".";
        }
    } else {
        Path = config.ParseResult->GetFreeArgs().at(argPos);
    }

    AdjustPath(config);
}

void TCommandWithPath::AdjustPath(const TClientCommand::TConfig& config) {
    if (!Path) {
        throw TMisuseException() << "Missing required argument <path>";
    }

    NConsoleClient::AdjustPath(Path, config);
}

void TCommandWithTopicName::ParseTopicName(const TClientCommand::TConfig &config, const size_t argPos) {
    TopicName = config.ParseResult->GetFreeArgs()[argPos];
}

}
}
