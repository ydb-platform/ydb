#pragma once

#include "command.h"

namespace NYdb {
namespace NConsoleClient {


// Format description:
//
// --option1    <Option1 description>
//
//              Option1 examples title:
//              1) Example 1 title
//              Example 1 text
//              2) Example 2 title
//              Example 2 text
//              ...
//
// --option2    <Option2 description>
//
//              Option2 examples title:
//              1) Example 1 title
//              Example 1 text
//              2) Example 2 title
//              Example 2 text
//              ...
//
// ...
//
// Command examples title:
// 1) Example 1 title
// Example 1 text
// 2) Example 2 title
// Example 2 text
//

struct TExample {
    TString Title;
    TString Text;
};

struct TExampleSet {
    TString Title;
    TVector<TExample> Examples;
};

class TExampleSetBuilder;

class TExampleBuilder {
public:
    explicit TExampleBuilder(TExampleSetBuilder& parent);

    // Optional
    TExampleBuilder& Title(const TString& title);
    TExampleBuilder& Text(const TString& text);
    TExampleSetBuilder& EndExample();

private:
    TExample Example;
    TExampleSetBuilder& Parent;
};

class TExampleSetBuilder {
public:
    TExampleSetBuilder() = default;

    // Optional. Default value : "Examples" ("Example" in case of a single example)
    TExampleSetBuilder& Title(const TString& title);
    TExampleBuilder BeginExample();

    TExampleSetBuilder& AddExample(TExample&& example);
    TExampleSet Build();

private:
    TExampleSet ExampleSet;
};

class TCommandWithExamples {
protected:
    void AddExamplesOption(TClientCommand::TConfig& config);

    void AddOptionExamples(const TString& optionName, TExampleSet&& examples);
    void AddCommandExamples(TExampleSet&& examples);

    // Should be called at the end of Config method of inherited commands when all other options are already added
    // This method adds example texts to option descriptions if --help-ex option was used
    void CheckExamples(const TClientCommand::TConfig& config);

private:
    bool IsOptionCalled(const TClientCommand::TConfig& config) const;

    THashMap<TString, TExampleSet> OptionExamples;
    TExampleSet CommandExamples;
};

}
}
