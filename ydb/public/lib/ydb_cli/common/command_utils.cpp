#include "command_utils.h"

namespace NYdb::NConsoleClient::NUtils {

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
