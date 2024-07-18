#include "command.h"
#include "normalize_path.h"

#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb {
namespace NConsoleClient {

bool TClientCommand::TIME_REQUESTS = false; // measure time of requests
bool TClientCommand::PROGRESS_REQUESTS = false; // display progress of long requests

namespace {
    TString FormatOption(const NLastGetopt::TOpt* option, const NColorizer::TColors& colors) {
        using namespace NLastGetopt;
        TStringStream result;
        const TOpt::TShortNames& shorts = option->GetShortNames();
        const TOpt::TLongNames& longs = option->GetLongNames();

        const size_t nopts = shorts.size() + longs.size();
        const bool multiple = 1 < nopts;
        if (multiple) {
            result << '{';
        }
        for (size_t i = 0; i < nopts; ++i) {
            if (multiple && 0 != i) {
                result << '|';
            }

            if (i < shorts.size()) { // short
                result << colors.GreenColor() << '-' << shorts[i] << colors.OldColor();
            } else {
                result << colors.GreenColor() << "--" << longs[i - shorts.size()] << colors.OldColor();
            }
        }
        if (multiple) {
            result << '}';
        }

        return result.Str();
    }

    // Option not to show in parent command help
    bool NeedToHideOption(const NLastGetopt::TOpt* opt) {
        if (opt->IsHidden()) {
            return true;
        }
        for (const char shortName : opt->GetShortNames()) {
            if (shortName == 'V' || shortName == 'h')
                return true;
        }
        return false;
    }

    void PrintOptionsDescription(IOutputStream& os, const NLastGetopt::TOpts* opts, NColorizer::TColors& colors, const TString& command) {
        using namespace NLastGetopt;
        NColorizer::TColors disabledColors(false);
        os << "  ";
        bool firstPrintedOption = true;
        for (size_t i = 0; i < opts->Opts_.size(); i++) {
            const TOpt* opt = opts->Opts_[i].Get();
            if (NeedToHideOption(opt)) {
                continue;
            }
            if (!firstPrintedOption) {
                os << ", ";
            }
            os << FormatOption(opt, colors);
            firstPrintedOption = false;
        }

        os << Endl << "  To get full description of these options run '" << command << "--help'.";
    }

    void PrintParentOptions(TStringStream& stream, TClientCommand::TConfig& config, NColorizer::TColors& colors) {
        bool foundRootParent = false;
        TStringBuilder fullCommand;
        for (const auto& parentCommand: config.ParentCommands) {
            fullCommand << parentCommand.Name << " ";
            if (parentCommand.Options) {
                TString name = "Global";
                if (!foundRootParent) {
                    foundRootParent = true;
                } else {
                    name = parentCommand.Name;
                    name[0] = toupper(name[0]);
                    stream << Endl << Endl;
                }
                stream << colors.BoldColor() << name << " options" << colors.OldColor() << ":" << Endl;
                PrintOptionsDescription(stream, parentCommand.Options, colors, fullCommand);
            }
        }
    }
}

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
            } else {
                opt = config.Opts->FindCharOption(optName[1]);
            }
            if (opt != nullptr && opt->GetHasArg() != NLastGetopt::NO_ARGUMENT) {
                if (eqPos == TStringBuf::npos) {
                    ++_argc;
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
            opt = config.Opts->FindCharOption(optName[1]);
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
    config.ArgsSettings.Reset(new TConfig::TArgSettings());
    config.Opts = &Opts;
    Config(config);
    CheckForExecutableOptions(config);
    config.CheckParamsCount();
    SetCustomUsage(config);
    SaveParseResult(config);
    config.ParseResult = ParseResult.get();
    Parse(config);
}

int TClientCommand::ValidateAndRun(TConfig& config) {
    config.Opts = &Opts;
    config.ParseResult = ParseResult.get();
    Validate(config);
    return Run(config);
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
    if (Hidden) {
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
    stream << prefix << colors.BoldColor() << Name << colors.OldColor();
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
    if (config.ParseResult->GetFreeArgCount() < argPos + 1 && isPathOptional) {
        if (isPathOptional) {
            Path = ".";
        }
    } else {
        Path = config.ParseResult->GetFreeArgs()[argPos];
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
