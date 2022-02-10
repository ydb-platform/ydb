#include "examples.h"

namespace NYdb {
namespace NConsoleClient {

TExampleBuilder::TExampleBuilder(TExampleSetBuilder& parent)
    : Parent(parent)
{}

TExampleBuilder& TExampleBuilder::Title(const TString& title) {
    Example.Title = title;
    return *this;
}

TExampleBuilder& TExampleBuilder::Text(const TString& text) {
    Example.Text = text;
    return *this;
}

TExampleSetBuilder& TExampleBuilder::EndExample() {
    return Parent.AddExample(std::move(Example));
}

/////////////////////////////////////////////////////////////////

TExampleSetBuilder& TExampleSetBuilder::Title(const TString& title) {
    ExampleSet.Title = title;
    return *this;
}

TExampleSetBuilder& TExampleSetBuilder::AddExample(TExample&& example) {
    ExampleSet.Examples.push_back(std::move(example));
    return *this;
}

TExampleBuilder TExampleSetBuilder::BeginExample() {
    return TExampleBuilder(*this);
}

TExampleSet TExampleSetBuilder::Build() {
    return std::move(ExampleSet);
}

/////////////////////////////////////////////////////////////////

void TCommandWithExamples::AddExamplesOption(TClientCommand::TConfig& config) {
    config.Opts->AddLongOption("help-ex", "Print usage with examples")
        .HasArg(NLastGetopt::NO_ARGUMENT)
        .IfPresentDisableCompletion()
        .Handler(&NLastGetopt::PrintUsageAndExit);
}

void TCommandWithExamples::AddOptionExamples(const TString& optionName, TExampleSet&& examples) {
    OptionExamples[optionName] = std::move(examples);
}

void TCommandWithExamples::AddCommandExamples(TExampleSet&& examples) {
    CommandExamples = std::move(examples);
}

namespace {
    void PrintExamples(TStringStream& descr, const TVector<TExample>& examples) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        if (examples.size() == 1) {
            if (examples[0].Title) {
                descr << examples[0].Title << ':' << Endl;
            }
            descr << examples[0].Text << Endl;
        } else {
            size_t orderNum = 0;
            for (const auto& example : examples) {
                descr << colors.BoldColor() << ++orderNum << ") " << colors.OldColor();
                if (example.Title) {
                    descr << example.Title << Endl;
                }
                descr << example.Text << Endl;
            }
        }
    }
}

void TCommandWithExamples::CheckExamples(const TClientCommand::TConfig& config) {
    if (!IsOptionCalled(config)) {
        return;
    }

    for (const auto& [optionName, examples] : OptionExamples) {
        NLastGetopt::TOpt* opt = config.Opts->FindLongOption(optionName);
        if (!opt) {
            continue;
        }
        TStringStream descr;
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        descr << opt->Help_ << Endl << Endl << colors.BoldColor();
        if (examples.Title) {
            descr << examples.Title;
        } else {
            descr << "Example";
            if (examples.Examples.size() > 1) {
                descr << 's';
            }
        }
        descr << colors.OldColor() << ":" << Endl;
        PrintExamples(descr, examples.Examples);
        opt->Help_ = descr.Str();
    }

    if (CommandExamples.Examples.size()) {
        TStringStream descr;
        PrintExamples(descr, CommandExamples.Examples);
        config.Opts->AddSection(CommandExamples.Title ? CommandExamples.Title
                : (CommandExamples.Examples.size() == 1 ? "Example" : "Examples"),
            descr.Str());
    }
}

bool TCommandWithExamples::IsOptionCalled(const TClientCommand::TConfig& config) const {
    for (int i = 0; i < config.ArgC; ++i) {
        TString argName = config.ArgV[i];
        if (argName == "--help-ex") {
            return true;
        }
    }
    return false;
}

}
}
