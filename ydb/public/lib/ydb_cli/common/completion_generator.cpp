#include "completion_generator.h"

#include <util/generic/overloaded.h>

#include <util/string/ascii.h>
#include <util/generic/hash_set.h>


#include <library/cpp/getopt/small/last_getopt_parse_result.h>

using NLastGetoptFork::NEscaping::Q;
using NLastGetoptFork::NEscaping::QQ;
using NLastGetoptFork::NEscaping::C;
using NLastGetoptFork::NEscaping::CC;
using NLastGetoptFork::NEscaping::S;
using NLastGetoptFork::NEscaping::SS;
using NLastGetoptFork::NEscaping::B;
using NLastGetoptFork::NEscaping::BB;

namespace NLastGetoptFork {

#define L out.Line()
#define I auto Y_GENERATE_UNIQUE_ID(indent) = out.Indent()

    TCompletionGenerator::TCompletionGenerator(const TModChooser* modChooser, const TOpts* opts)
        : chooser(modChooser), opts(opts)
    {
        Y_ABORT_UNLESS(modChooser != nullptr);
        Y_ABORT_UNLESS(opts != nullptr);
    }

    TCompletionGenerator::TCompletionGenerator(const TModChooser* modChooser)
        : chooser(modChooser), opts(nullptr)
    {
        Y_ABORT_UNLESS(modChooser != nullptr);
    }

    TCompletionGenerator::TCompletionGenerator(const TOpts* opts)
        : chooser(nullptr), opts(opts)
    {
        Y_ABORT_UNLESS(opts != nullptr);
    }

    void TZshCompletionGenerator::Generate(TStringBuf command, IOutputStream& stream) {
        TFormattedOutput out;
        NComp::TCompleterManager manager{command};

        L << "#compdef " << command;
        L;
        L << "_" << command << "() {";
        {
            I;
            L << "local state line desc modes context curcontext=\"$curcontext\" ret=1";
            L << "local words_orig=(\"${words[@]}\")";
            L << "local current_orig=\"$((CURRENT - 1))\"";
            L << "local prefix_orig=\"$PREFIX\"";
            L << "local suffix_orig=\"$SUFFIX\"";
            L;
            if (chooser != nullptr && opts != nullptr) {
                GenerateBothCompletion(out, *chooser, *opts, manager);
            } else if (chooser != nullptr) {
                GenerateModesCompletion(out, *chooser, manager);
            } else if (opts != nullptr) {
                GenerateOptsCompletion(out, *opts, manager);
            }
            L;
            L << "return ret";
        }
        L << "}";
        L;
        manager.GenerateZsh(out);

        out.Print(stream);
    }

    void TZshCompletionGenerator::GenerateBothCompletion(TFormattedOutput& out, const TModChooser& chooser, const TOpts& opts, NComp::TCompleterManager& manager) {
        L << "_arguments -s -C \\";
        {
            I;

            if (opts.ArgPermutation_ == EArgPermutation::REQUIRE_ORDER) {
                L << "-S \\";
            }

            for (auto opt: opts.GetOpts()) {
                if (!opt->Hidden_) {
                    GenerateOptCompletion(out, opts, *opt, manager);
                }
            }

            L << "  '()1: :->modes' \\";
            L << "  '()*:: :->args' \\";
            L << "&& ret=0";
        }
        
        auto modes = chooser.GetUnsortedModes();

        L;
        L << "case $state in";
        {
            I;

            L << "modes)";
            {
                I;

                size_t tag = 0;
                bool empty = true;

                L << "desc='modes'";
                L << "modes=(";
                for (auto& mode : modes) {
                    if (mode->Hidden) {
                        continue;
                    }
                    if (!mode->Name.empty()) {
                        I;
                        if (!mode->Description.empty()) {
                            L << QQ(mode->Name) << ":" << QQ(mode->Description);
                        } else {
                            L << QQ(mode->Name);
                        }
                        empty = false;
                    } else {
                        L << ")";
                        if (!empty) {
                            L << "_describe -t 'mode-group-" << tag << "' $desc modes";
                        }
                        L;
                        if (mode->Description.empty()) {
                            L << "desc='modes'";
                        } else {
                            L << "desc=" << SS(mode->Description);
                        }
                        L << "modes=(";
                        empty = true;
                        ++tag;
                    }
                }
                L << ")";
                if (!empty) {
                    L << "_describe -t 'mode-group-" << tag << "' $desc modes";
                }
                L;

                L << ";;";
            }

            L << "args)";
            {
                I;

                L << "case $line[1] in";
                {
                    I;

                    for (auto& mode : modes) {
                        if (mode->Name.empty() || mode->Hidden) {
                            continue;
                        }

                        auto& line = L << SS(mode->Name);
                        for (auto& alias : mode->Aliases) {
                            line << "|" << SS(alias);
                        }
                        line << ")";

                        {
                            I;

                            auto mainArgs = dynamic_cast<TMainClassArgs*>(mode->Main);
                            auto mainModes = dynamic_cast<TMainClassModes*>(mode->Main);
                            if (mainArgs && mainModes) {
                                GenerateBothCompletion(out, mainModes->GetSubModes(), mainArgs->GetOptions(), manager);
                            } else if (mainArgs) {
                                GenerateOptsCompletion(out, mainArgs->GetOptions(), manager);
                            } else if (mainModes) {
                                GenerateModesCompletion(out, mainModes->GetSubModes(), manager);
                            } else {
                                GenerateDefaultOptsCompletion(out, manager);
                            }

                            L << ";;";
                        }
                    }
                }
                L << "esac";
                L << ";;";
            }
        }
        L << "esac";
    }

    void TZshCompletionGenerator::GenerateModesCompletion(TFormattedOutput& out, const TModChooser& chooser, NComp::TCompleterManager& manager) {
        auto modes = chooser.GetUnsortedModes();

        L << "_arguments -C \\";
        L << "  '(- : *)'{-h,--help}'[show help information]' \\";
        if (chooser.GetVersionHandler() != nullptr) {
            L << "  '(- : *)'{-v,--version}'[display version information]' \\";
        }
        if (!chooser.IsSvnRevisionOptionDisabled()) {
            L << "  '(- : *)--svnrevision[show build information]' \\";
        }
        L << "  '(-v --version -h --help --svnrevision)1: :->modes' \\";
        L << "  '(-v --version -h --help --svnrevision)*:: :->args' \\";
        L << "  && ret=0";
        L;
        L << "case $state in";
        {
            I;

            L << "modes)";
            {
                I;

                size_t tag = 0;
                bool empty = true;

                L << "desc='modes'";
                L << "modes=(";
                for (auto& mode : modes) {
                    if (mode->Hidden) {
                        continue;
                    }
                    if (!mode->Name.empty()) {
                        I;
                        if (!mode->Description.empty()) {
                            L << QQ(mode->Name) << ":" << QQ(mode->Description);
                        } else {
                            L << QQ(mode->Name);
                        }
                        empty = false;
                    } else {
                        L << ")";
                        if (!empty) {
                            L << "_describe -t 'mode-group-" << tag << "' $desc modes";
                        }
                        L;
                        if (mode->Description.empty()) {
                            L << "desc='modes'";
                        } else {
                            L << "desc=" << SS(mode->Description);
                        }
                        L << "modes=(";
                        empty = true;
                        ++tag;
                    }
                }
                L << ")";
                if (!empty) {
                    L << "_describe -t 'mode-group-" << tag << "' $desc modes";
                }
                L;

                L << ";;";
            }

            L << "args)";
            {
                I;

                L << "case $line[1] in";
                {
                    I;

                    for (auto& mode : modes) {
                        if (mode->Name.empty() || mode->Hidden) {
                            continue;
                        }

                        auto& line = L << SS(mode->Name);
                        for (auto& alias : mode->Aliases) {
                            line << "|" << SS(alias);
                        }
                        line << ")";

                        {
                            I;
                            
                            auto mainArgs = dynamic_cast<TMainClassArgs*>(mode->Main);
                            auto mainModes = dynamic_cast<TMainClassModes*>(mode->Main);
                            if (mainArgs && mainModes) {
                                GenerateBothCompletion(out, mainModes->GetSubModes(), mainArgs->GetOptions(), manager);
                            } else if (mainArgs) {
                                GenerateOptsCompletion(out, mainArgs->GetOptions(), manager);
                            } else if (mainModes) {
                                GenerateModesCompletion(out, mainModes->GetSubModes(), manager);
                            } else {
                                GenerateDefaultOptsCompletion(out, manager);
                            }

                            L << ";;";
                        }
                    }
                }
                L << "esac";
                L << ";;";
            }
        }
        L << "esac";
    }

    void TZshCompletionGenerator::GenerateOptsCompletion(TFormattedOutput& out, const TOpts& opts, NComp::TCompleterManager& manager) {
        L << "_arguments -s \\";
        {
            I;

            if (opts.ArgPermutation_ == EArgPermutation::REQUIRE_ORDER) {
                L << "-S \\";
            }

            for (auto opt: opts.GetOpts()) {
                if (!opt->Hidden_) {
                    GenerateOptCompletion(out, opts, *opt, manager);
                }
            }

            auto argSpecs = opts.GetFreeArgSpecs();
            size_t numFreeArgs = opts.GetFreeArgsMax();
            bool unlimitedArgs = false;
            if (numFreeArgs == TOpts::UNLIMITED_ARGS) {
                numFreeArgs = argSpecs.empty() ? 0 : (argSpecs.rbegin()->first + 1);
                unlimitedArgs = true;
            }

            for (size_t i = 0; i < numFreeArgs; ++i) {
                auto& spec = argSpecs[i];
                auto& line = L << "'" << (i + 1) << ":";
                if (spec.IsOptional()) {
                    line << ":";
                }
                auto argHelp = spec.GetCompletionArgHelp(opts.GetDefaultFreeArgTitle());
                if (argHelp) {
                    line << Q(argHelp);
                } else {
                    line << " ";
                }
                line << ":";
                if (spec.Completer_) {
                    line << spec.Completer_->GenerateZshAction(manager);
                } else {
                    line << "_default";
                }
                line << "' \\";
            }

            if (unlimitedArgs) {
                auto& spec = opts.GetTrailingArgSpec();
                auto& line = L << "'*:";
                auto argHelp = spec.GetCompletionArgHelp(opts.GetDefaultFreeArgTitle());
                if (argHelp) {
                    line << Q(argHelp);
                } else {
                    line << " ";
                }
                line << ":";
                if (spec.Completer_) {
                    line << spec.Completer_->GenerateZshAction(manager);
                } else {
                    line << "_default";
                }
                line << "' \\";
            }

            L << "&& ret=0";
        }
    }

    void TZshCompletionGenerator::GenerateDefaultOptsCompletion(TFormattedOutput& out, NComp::TCompleterManager&) {
        L << "_arguments \\";
        L << "  '(- *)'{-h,--help}'[show help information]' \\";
        L << "  '(- *)--svnrevision[show build information]' \\";
        L << "  '(-h --help --svnrevision)*: :_files' \\";
        L << "  && ret=0";
    }

    void TZshCompletionGenerator::GenerateOptCompletion(TFormattedOutput& out, const TOpts& opts, const TOpt& opt, NComp::TCompleterManager& manager) {
        auto& line = L;

        THashSet<TString> disableOptions;
        if (opt.DisableCompletionForOptions_) {
            disableOptions.insert("-");
        } else {
            if (!opt.AllowMultipleCompletion_) {
                for (auto shortName: opt.GetShortNames()) {
                    disableOptions.insert(TString("-") + shortName);
                }
                for (auto& longName: opt.GetLongNames()) {
                    disableOptions.insert("--" + longName);
                }
            }
            for (auto disabledShortName : opt.DisableCompletionForChar_) {
                auto disabledOpt = opts.FindCharOption(disabledShortName);
                if (disabledOpt) {
                    for (auto shortName: disabledOpt->GetShortNames()) {
                        disableOptions.insert(TString("-") + shortName);
                    }
                    for (auto& longName: disabledOpt->GetLongNames()) {
                        disableOptions.insert("--" + longName);
                    }
                } else {
                    disableOptions.insert(TString("-") + disabledShortName);
                }
            }
            for (auto& disabledLongName : opt.DisableCompletionForLongName_) {
                auto disabledOpt = opts.FindLongOption(disabledLongName);
                if (disabledOpt) {
                    for (auto shortName: disabledOpt->GetShortNames()) {
                        disableOptions.insert(TString("-") + shortName);
                    }
                    for (auto& longName: disabledOpt->GetLongNames()) {
                        disableOptions.insert("--" + longName);
                    }
                } else {
                    disableOptions.insert("--" + disabledLongName);
                }
            }
        }
        if (opt.DisableCompletionForFreeArgs_) {
            disableOptions.insert(":");
            disableOptions.insert("*");
        } else {
            for (auto i : opt.DisableCompletionForFreeArg_) {
                disableOptions.insert(ToString(i + 1));
            }
        }

        TStringBuf sep = "";

        if (!disableOptions.empty()) {
            line << "'(";
            for (auto& disableOption : disableOptions) {
                line << sep << disableOption;
                sep = " ";
            }
            line << ")";
        }

        sep = "";
        TStringBuf mul = "";
        TStringBuf quot = "";

        if (opt.GetShortNames().size() + opt.GetLongNames().size() > 1) {
            if (!disableOptions.empty()) {
                line << "'";
            }
            line << "{";
            quot = "'";
        } else {
            if (disableOptions.empty()) {
                line << "'";
            }
        }

        if (opt.AllowMultipleCompletion_) {
            mul = "*";
        }

        for (auto& flag : opt.GetShortNames()) {
            line << sep << quot << mul << "-" << Q(TStringBuf(&flag, 1)) << quot;
            sep = ",";
        }

        for (auto& flag : opt.GetLongNames()) {
            line << sep << quot << mul << "--" << Q(flag) << quot;
            sep = ",";
        }

        if (opt.GetShortNames().size() + opt.GetLongNames().size() > 1) {
            line << "}'";
        }

        if (opt.GetCompletionHelp()) {
            line << "[";
            line << Q(opt.GetCompletionHelp());
            line << "]";
        }

        if (opt.HasArg_ != EHasArg::NO_ARGUMENT) {
            if (opt.HasArg_ == EHasArg::OPTIONAL_ARGUMENT) {
                line << ":";
            }

            line << ":";

            if (opt.GetCompletionArgHelp()) {
                line << C(opt.GetCompletionArgHelp());
            } else {
                line << " ";
            }

            line << ":";

            if (opt.Completer_) {
                line << C(opt.Completer_->GenerateZshAction(manager));
            } else {
                line << "_default";
            }
        }

        line << "' \\";
    }

    void TBashCompletionGenerator::Generate(TStringBuf command, IOutputStream& stream) {
        TFormattedOutput out;
        NComp::TCompleterManager manager{command};

        L << "_" << command << "() {";
        {
            I;
            L << "COMPREPLY=()";
            L;
            L << "local i args opts items candidates";
            L;
            L << "local cur prev words cword";
            L << "_get_comp_words_by_ref -n \"\\\"'><=;|&(:\" cur prev words cword";
            L;
            L << "local need_space=\"1\"";
            L << "declare disable_completion_for_options=\"0\"";
            L << "declare disable_completion_for_args=\"0\"";
            L << "declare -A disabled_options=()";
            L << "declare -A disabled_args=()";
            L << "local IFS=$' \\t\\n'";
            L;    
            if (chooser != nullptr && opts != nullptr) {
                GenerateBothCompletion(out, *chooser, *opts, manager, 1);
            } else if (chooser != nullptr) {
                GenerateModesCompletion(out, *chooser, manager, 1);
            } else if (opts != nullptr) {
                GenerateOptsCompletion(out, *opts, manager, 1);
            }
            L;
            L << "__ltrim_colon_completions \"$cur\"";
            L;
            L << "IFS=$'\\n'";
            L << "if [ ${#COMPREPLY[@]} -ne 0 ]; then";
            {
                I;
                L << "if [[ -z $need_space ]]; then";
                {
                    I;
                    L << "COMPREPLY=( $(printf \"%q\\n\" \"${COMPREPLY[@]}\") )";
                }
                L << "else";
                {
                    I;
                    L << "COMPREPLY=( $(printf \"%q \\n\" \"${COMPREPLY[@]}\") )";
                }
                L << "fi";
            }
            L << "fi";
            L;
            L << "return 0";
        }
        L << "}";
        L;
        L << "complete -o nospace -o default -F _" << command << " " << command;

        out.Print(stream);
    }

    void TBashCompletionGenerator::GenerateBothCompletion(TFormattedOutput& out, const TModChooser& chooser, const TOpts& opts, NComp::TCompleterManager& manager, size_t level) {
        auto modes = chooser.GetUnsortedModes();
        auto unorderedOpts = opts.GetOpts();

        L << "disable_completion_for_options=\"0\"";
        L << "disable_completion_for_args=\"0\"";
        L << "disabled_options=()";
        L << "disabled_args=()";
        L;
        L << "while true; do";
        {
            I;
            L << "if [[ ${cword} == " << level << " ]] ; then";
            {
                I;
                L << "if [[ ${cur} == -* ]] ;  then";
                {
                    I;
                    L << "if ! [[ ${disable_completion_for_options} == 1 ]] ; then";
                    {
                        I;
                        auto& line = L << "local words=(";
                        TStringBuf sep = "";
                        for (auto& opt : unorderedOpts) {
                            if (opt->IsHidden()) {
                                continue;
                            }

                            for (auto& shortName : opt->GetShortNames()) {
                                line << sep << "'-" << B(TStringBuf(&shortName, 1)) << "'";
                                sep = " ";
                            }
                            for (auto& longName: opt->GetLongNames()) {
                                line << sep << "'--" << B(longName) << "'";
                                sep = " ";
                            }
                        }
                        line << ")";
                        L << "for word in \"${words[@]}\"; do";
                        {
                            I;    
                            L << "if ! [[ -v disabled_options[\"$word\"] ]] ; then";
                            {
                                I;
                                L << "COMPREPLY+=( $(compgen -W \"$word\" -- ${cur}) )";
                            }
                            L << "fi";
                        }
                        L << "done";
                    }
                    L << "fi";
                }
                L << "else";
                {
                    I;
                    L << "if ! [[ ${disable_completion_for_args} == 1 ]] ; then";
                    {
                        I;
                        auto& line = L << "COMPREPLY+=( $(compgen -W '";
                        TStringBuf sep = "";
                        for (auto& mode : modes) {
                            if (!mode->Hidden && !mode->NoCompletion) {
                                line << sep << B(mode->Name);
                                sep = " ";
                            }
                        }
                        line << "' -- ${cur}) )";
                    }
                    L << "fi";
                }
                L << "fi";
            }
            L << "else";
            {
                I;
                L << "case \"${words[" << level << "]}\" in";
                {
                    I;

                    for (auto& opt : unorderedOpts) {
                        if (opt->IsHidden()) {
                            continue;
                        }
                        GenerateOptCompletion(out, opts, *opt, manager, level);
                    }
                    
                    for (auto& mode : modes) {
                        if (mode->Name.empty() || mode->Hidden || mode->NoCompletion) {
                            continue;
                        }

                        auto& line = L << BB(mode->Name);
                        for (auto& alias : mode->Aliases) {
                            line << "|" << BB(alias);
                        }
                        line << ")";
                        {
                            I;
                            auto mainArgs = dynamic_cast<TMainClassArgs*>(mode->Main);
                            auto mainModes = dynamic_cast<TMainClassModes*>(mode->Main);
                            if (mainArgs && mainModes) {
                                GenerateBothCompletion(out, mainModes->GetSubModes(), mainArgs->GetOptions(), manager, level + 1);
                            } else if (mainArgs) {
                                GenerateOptsCompletion(out, mainArgs->GetOptions(), manager, level + 1);
                            } else if (mainModes) {
                                GenerateModesCompletion(out, mainModes->GetSubModes(), manager, level + 1);
                            } else {
                                GenerateDefaultOptsCompletion(out, manager);
                            }

                            L << ";;";
                        }
                    }
                L << "esac";
                }
            }
            L << "fi";
            L << "break";
        }
        L << "done";
    }

    void TBashCompletionGenerator::GenerateOptCompletion(TFormattedOutput& out, const TOpts& opts, const TOpt& opt, NComp::TCompleterManager&, size_t level) {
        auto& line = L;
        TStringBuf sep = "";
        for (auto& shortName : opt.GetShortNames()) {
            line << sep << "'-" << B(TStringBuf(&shortName, 1)) << "'";
            sep = "|";
        }
        for (auto& longName: opt.GetLongNames()) {
            line << sep << "'--" << B(longName) << "'";
            sep = "|";
        }
        line << ")";
        {
            I;
            if (opt.DisableCompletionForOptions_) {
                L << "disable_completion_for_options=\"1\"";
            }
            if (opt.DisableCompletionForFreeArgs_) {
                L << "disable_completion_for_args=\"1\"";
            }
            
            for (auto disabledShortName : opt.DisableCompletionForChar_) {
                auto disabledOpt = opts.FindCharOption(disabledShortName);
                if (disabledOpt) {
                    for (auto shortName: disabledOpt->GetShortNames()) {
                        L << "disabled_options[\"-" << shortName << "\"]=\"1\"";
                    }
                    for (auto& longName: disabledOpt->GetLongNames()) {
                        L << "disabled_options[\"-" << longName << "\"]=\"1\"";
                    }
                } else {
                    L << "disabled_options[\"-" << disabledShortName << "\"]=\"1\"";
                }

            }
            for (auto& disabledLongName : opt.DisableCompletionForLongName_) {
                auto disabledOpt = opts.FindLongOption(disabledLongName);
                if (disabledOpt) {
                    for (auto shortName: disabledOpt->GetShortNames()) {
                        L << "disabled_options[\"--" << shortName << "\"]=\"1\"";
                    }
                    for (auto& longName: disabledOpt->GetLongNames()) {
                        L << "disabled_options[\"--" << longName << "\"]=\"1\"";
                    }
                } else {
                L << "disabled_options[\"--" << disabledLongName << "\"]=\"1\"";
                }
            }
            for (auto disabledArgIndex : opt.DisableCompletionForFreeArg_) {
                L << "disabled_args[" << disabledArgIndex << "]=\"1\"";
            }
            if (!opt.AllowMultipleCompletion_) {
                for (auto& shortName : opt.GetShortNames()) {
                    L << "disabled_options[\"-" << shortName << "\"]=\"1\"";
                }
                for (auto& longName: opt.GetLongNames()) {
                    L << "disabled_options[\"--" << longName << "\"]=\"1\"";
                }
            }                        

            if (opt.HasArg_ == EHasArg::NO_ARGUMENT) {
                // pop option from words
                L << "cword=$((cword-1))";
                L << "words=(\"${words[@]:0:" << level << "}\" \"${words[@]:" << level + 1 << "}\")";
                L << "continue";
            } else if (opt.HasArg_ == EHasArg::REQUIRED_ARGUMENT) {
                L << "if [[ ${cword} == " << level + 1 << " ]] ; then";
                {
                    I;
                    if (opt.Completer_ != nullptr) {
                        opt.Completer_->GenerateBash(out);
                    } else {
                        L << ": # no-op: no completer for option";
                    }
                }
                L << "else";
                {
                    I;
                    // pop option and its argument from words
                    L << "cword=$((cword-2))";
                    L << "words=(\"${words[@]:0:" << level << "}\" \"${words[@]:" << level + 2 << "}\")";
                    L << "continue";
                }          
                L << "fi";                      
            } else if (opt.HasArg_ == EHasArg::OPTIONAL_ARGUMENT) {
                L << "if [[ ${cword} == " << level + 1 << " ]] ; then";
                {
                    I;
                    if (opt.Completer_ != nullptr) {
                        opt.Completer_->GenerateBash(out);
                    } else {
                        L << ": # no-op: no completer for option";
                    }
                }
                L << "else";
                {
                    I;
                    // check if option has argument set -- so that we know how many words to skip
                    L << "args=0";
                    auto& line = L << "opts='@(";
                    TStringBuf sep = "";
                    for (auto& opt : opts.GetOpts()) {
                        if (opt->IsHidden()) {
                            continue;
                        }
                        for (auto& shortName : opt->GetShortNames()) {
                            line << sep << "-" << B(TStringBuf(&shortName, 1));
                            sep = "|";
                        }
                        for (auto& longName: opt->GetLongNames()) {
                            line << sep << "--" << B(longName);
                            sep = "|";
                        }
                    }
                    line << ")'";
                    L << "for (( i=" << level + 1 << "; i < cword; i++ )); do";
                    {
                        I;
                        L << "if [[ ${words[i]} == $opts ]]; then";
                        {
                            I;
                            L << "break";                                        
                        }
                        L << "else";
                        {
                            I;
                            L << "(( args++ ))";
                        }
                        L << "fi";
                    }
                    L << "done";
                    L;

                    L << "if [[ $args == 0 ]] ; then ";
                    {
                        I;
                        // pop option from words
                        L << "cword=$((cword-1))";
                        L << "words=(\"${words[@]:0:" << level << "}\" \"${words[@]:" << level + 1 << "}\")";
                        L << "continue";
                    }
                    L << "else";
                    {
                        I;
                        // pop option and its argument from words
                        L << "cword=$((cword-2))";
                        L << "words=(\"${words[@]:0:" << level << "}\" \"${words[@]:" << level + 2 << "}\")";
                        L << "continue";
                    }
                    L << "fi";
                }
                L << "fi";
            }
        }
        L << ";;";
    }

    void TBashCompletionGenerator::GenerateModesCompletion(TFormattedOutput& out, const TModChooser& chooser, NComp::TCompleterManager& manager, size_t level) {
        auto modes = chooser.GetUnsortedModes();

        L << "if [[ ${cword} == " << level << " ]] ; then";
        {
            I;
            L << "if [[ ${cur} == -* ]] ;  then";
            {
                I;
                auto& line = L << "COMPREPLY+=( $(compgen -W '-h --help";
                if (chooser.GetVersionHandler() != nullptr) {
                    line << " -v --version";
                }
                if (!chooser.IsSvnRevisionOptionDisabled()) {
                    line << " --svnrevision";
                }
                line << "' -- ${cur}) )";
            }
            L << "else";
            {
                I;
                auto& line = L << "COMPREPLY+=( $(compgen -W '";
                TStringBuf sep = "";
                for (auto& mode : modes) {
                    if (!mode->Hidden && !mode->NoCompletion) {
                        line << sep << B(mode->Name);
                        sep = " ";
                    }
                }
                line << "' -- ${cur}) )";
            }
            L << "fi";
        }
        L << "else";
        {
            I;
            L << "case \"${words[" << level << "]}\" in";
            {
                I;

                for (auto& mode : modes) {
                    if (mode->Name.empty() || mode->Hidden || mode->NoCompletion) {
                        continue;
                    }

                    auto& line = L << BB(mode->Name);
                    for (auto& alias : mode->Aliases) {
                        line << "|" << BB(alias);
                    }
                    line << ")";

                    {
                        I;

                        if (auto mainArgs = dynamic_cast<TMainClassArgs*>(mode->Main)) {
                            GenerateOptsCompletion(out, mainArgs->GetOptions(), manager, level + 1);
                        } else if (auto mainModes = dynamic_cast<TMainClassModes*>(mode->Main)) {
                            GenerateModesCompletion(out, mainModes->GetSubModes(), manager, level + 1);
                        } else {
                            GenerateDefaultOptsCompletion(out, manager);
                        }

                        L << ";;";
                    }
                }
            }
            L << "esac";
        }
        L << "fi";
    }

    void TBashCompletionGenerator::GenerateOptsCompletion(TFormattedOutput& out, const TOpts& opts, NComp::TCompleterManager& manager, size_t level) {
        auto unorderedOpts = opts.GetOpts();
        L << "disable_completion_for_options=\"0\"";
        L << "disable_completion_for_args=\"0\"";
        L << "disabled_options=()";
        L << "disabled_args=()";
        L;
        L << "while true; do";
        {
            I;

            L << "if [[ ${cword} == " << level << " ]] && [[ ${cur} == -* ]] ; then";
            {
                I;
                {
                    I;
                    L << "if ! [[ ${disable_completion_for_options} == 1 ]] ; then";
                    {
                        I;
                        auto& line = L << "local words=(";
                        TStringBuf sep = "";
                        for (auto& opt : unorderedOpts) {
                            if (opt->IsHidden()) {
                                continue;
                            }

                            for (auto& shortName : opt->GetShortNames()) {
                                line << sep << "'-" << B(TStringBuf(&shortName, 1)) << "'";
                                sep = " ";
                            }
                            for (auto& longName: opt->GetLongNames()) {
                                line << sep << "'--" << B(longName) << "'";
                                sep = " ";
                            }
                        }
                        line << ")";
                        L << "for word in \"${words[@]}\"; do";
                        {
                            I;    
                            L << "if ! [[ -v disabled_options[\"$word\"] ]] ; then";
                            {
                                I;
                                L << "COMPREPLY+=( $(compgen -W \"$word\" -- ${cur}) )";
                            }
                            L << "fi";
                        }
                        L << "done";
                    }
                    L << "fi";
                }                
            }
            L << "else";
            {
                I;
                L << "case \"${words[" << level << "]}\" in";
                {
                    I;
                    for (auto& opt : unorderedOpts) {
                        if (opt->IsHidden()) {
                            continue;
                        }
                        GenerateOptCompletion(out, opts, *opt, manager, level);
                    }

                    L << "*)";
                    {
                    I;
                    L << "if ! [[ ${disable_completion_for_args} == 1 ]] ; then";
                    {
                        I;
                        L << "args=0";
                        auto& line = L << "opts='@(";
                        TStringBuf sep = "";
                        for (auto& opt : unorderedOpts) {
                            if (opt->HasArg_ == EHasArg::NO_ARGUMENT || opt->IsHidden()) {
                                continue;
                            }
                            for (auto& shortName : opt->GetShortNames()) {
                                line << sep << "-" << B(TStringBuf(&shortName, 1));
                                sep = "|";
                            }
                            for (auto& longName: opt->GetLongNames()) {
                                line << sep << "--" << B(longName);
                                sep = "|";
                            }
                        }
                        line << ")'";
                        L << "for (( i=" << level << "; i < cword; i++ )); do";
                        {
                            I;
                            L << "if [[ ${words[i]} != -* && ${words[i-1]} != $opts ]]; then";
                            {
                                I;
                                L << "(( args++ ))";
                            }
                            L << "fi";
                        }
                        L << "done";
                        L;

                        auto argSpecs = opts.GetFreeArgSpecs();
                        size_t numFreeArgs = opts.GetFreeArgsMax();
                        bool unlimitedArgs = false;
                        if (numFreeArgs == TOpts::UNLIMITED_ARGS) {
                            numFreeArgs = argSpecs.empty() ? 0 : (argSpecs.rbegin()->first + 1);
                            unlimitedArgs = true;
                        }

                        L << "case ${args} in";
                        {
                            I;

                            for (size_t i = 0; i < numFreeArgs; ++i) {
                                L << i << ")";
                                {
                                    I;
                                    auto& spec = argSpecs[i];
                                    if (spec.Completer_ != nullptr) {
                                        L << "if ! [[ -v disabled_args[" << i << "] ]] ; then ";
                                        {
                                            I;
                                            spec.Completer_->GenerateBash(out);
                                        }
                                        L << "fi";
                                    }
                                    L << ";;";
                                }
                            }
                            if (unlimitedArgs) {
                                L << "*)";
                                {
                                    I;
                                    auto& spec = opts.GetTrailingArgSpec();
                                    if (spec.Completer_ != nullptr) {
                                        spec.Completer_->GenerateBash(out);
                                    }
                                    L << ";;";
                                }
                            }
                        }
                        L << "esac";
                    }
                    L << "fi";
                }
                }
                L << "esac";
            }
            L << "fi";
            L << "break";
        }
        L << "done";
    }

    void TBashCompletionGenerator::GenerateDefaultOptsCompletion(TFormattedOutput& out, NComp::TCompleterManager&) {
        L << "if [[ ${cur} == -* ]] ;  then";
        {
            I;
            L << "COMPREPLY+=( $(compgen -W '-h --help --svnrevision' -- ${cur}) )";
        }
        L << "fi";
    }

#undef I
#undef L

    TString NEscaping::Q(TStringBuf string) {
        TStringBuilder out;
        out.reserve(string.size());
        for (auto c: string) {
            switch (c) {
                case '\a':
                case '\b':
                case '\f':
                case '\n':
                case '\r':
                case '\t':
                case '\v':
                    out << " ";
                    break;
                case '\\':
                    out << "\\\\";
                    break;
                case '\'':
                    out << "''";
                    break;
                case '\"':
                    out << "\\\"";
                    break;
                case '[':
                    out << "\\[";
                    break;
                case ']':
                    out << "\\]";
                    break;
                case ':':
                    out << "\\:";
                    break;
                case '+':
                    out << "\\+";
                    break;
                case '=':
                    out << "\\=";
                    break;
                default:
                    out << c;
                    break;
            }
        }
        return out;
    }

    TString NEscaping::QQ(TStringBuf string) {
        auto q = Q(string);
        return "'" + q + "'";
    }

    TString NEscaping::C(TStringBuf string) {
        TStringBuilder out;
        out.reserve(string.size() + 1);
        for (auto c: string) {
            switch (c) {
                case '\a':
                case '\b':
                case '\f':
                case '\n':
                case '\r':
                case '\t':
                case '\v':
                    out << " ";
                    break;
                case '\'':
                    out << "''";
                    break;
                case ':':
                    out << "\\:";
                    break;
                default:
                    out << c;
                    break;
            }
        }
        return out;
    }

    TString NEscaping::CC(TStringBuf string) {
        auto c = C(string);
        return "'" + c + "'";
    }

    TString NEscaping::S(TStringBuf string) {
        TStringBuilder out;
        out.reserve(string.size() + 1);
        for (auto c: string) {
            switch (c) {
                case '\a':
                case '\b':
                case '\f':
                case '\n':
                case '\r':
                case '\t':
                case '\v':
                    out << " ";
                    break;
                case '\'':
                    out << "''";
                    break;
                default:
                    out << c;
                    break;
            }
        }
        return out;
    }

    TString NEscaping::SS(TStringBuf string) {
        auto s = S(string);
        return "'" + s + "'";
    }

    TString NEscaping::B(TStringBuf string) {
        TStringBuilder out;
        out.reserve(string.size() + 1);
        for (auto c: string) {
            switch (c) {
                case '\a':
                case '\b':
                case '\f':
                case '\n':
                case '\r':
                case '\t':
                case '\v':
                    out << " ";
                    break;
                case '\'':
                    out << "'\"'\"'";
                    break;
                default:
                    out << c;
                    break;
            }
        }
        return out;
    }

    TString NEscaping::BB(TStringBuf string) {
        auto b = B(string);
        return "'" + b + "'";
    }
}
