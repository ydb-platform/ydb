#include "completion_graph_json.h"

#include <library/cpp/json/writer/json.h>

#include <util/generic/algorithm.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

namespace {

using namespace NLastGetopt;

// Write the JSON value describing what the option accepts:
//   null         -- flag without a value (e.g. --help, --verbose),
//                   or option that takes an arbitrary value (URL, path, ...)
//   [v1, v2]     -- option with a fixed set of allowed values (TOpt::Choices),
//                   sorted lexicographically for stable output
void WriteOptValue(NJsonWriter::TBuf& json, const TOpt& opt) {
    if (opt.GetHasArg() == NO_ARGUMENT) {
        json.WriteNull();
        return;
    }
    // TOpt::Choices_ is a THashSet<TString>; GetChoicesHelp() is the only public
    // accessor and exposes its contents as ", "-joined string. Choices in YDB CLI
    // are plain identifiers (e.g. "csv", "json"), so splitting by ", " is safe in
    // practice. Output is sorted to keep the JSON deterministic across runs.
    const TString choicesHelp = opt.GetChoicesHelp();
    if (choicesHelp.empty()) {
        json.WriteNull();
        return;
    }
    TVector<TString> values;
    StringSplitter(choicesHelp).SplitByString(", ").SkipEmpty().Collect(&values);
    for (TString& value : values) {
        value = StripString(value);
    }
    Sort(values);
    json.BeginList();
    for (const TString& value : values) {
        json.WriteString(value);
    }
    json.EndList();
}

void WriteOpts(NJsonWriter::TBuf& json, const TOpts& opts) {
    // Collect (key, opt) pairs and sort by key so that the resulting JSON does
    // not depend on the order in which AddLongOption()/AddShortOption() were called.
    TVector<std::pair<TString, const TOpt*>> entries;
    for (const TOpt* opt : opts.GetOpts()) {
        if (opt->IsHidden()) {
            continue;
        }
        for (const TString& name : opt->GetLongNames()) {
            entries.emplace_back("--" + name, opt);
        }
        for (char ch : opt->GetShortNames()) {
            entries.emplace_back(TString("-") + ch, opt);
        }
    }
    SortBy(entries, [](const auto& e) { return e.first; });
    json.WriteKey("options").BeginObject();
    for (const auto& [key, opt] : entries) {
        json.WriteKey(key);
        WriteOptValue(json, *opt);
    }
    json.EndObject();
}

void WriteModes(NJsonWriter::TBuf& json, const TModChooser& chooser);

void WriteNodeBody(NJsonWriter::TBuf& json, TMainClass* main) {
    auto* mainModes = dynamic_cast<TMainClassModes*>(main);
    auto* mainArgs = dynamic_cast<TMainClassArgs*>(main);
    if (mainModes) {
        WriteModes(json, mainModes->GetSubModes());
    } else {
        json.WriteKey("handlers").BeginObject().EndObject();
    }
    if (mainArgs) {
        WriteOpts(json, mainArgs->GetOptions());
    } else {
        json.WriteKey("options").BeginObject().EndObject();
    }
}

void WriteModes(NJsonWriter::TBuf& json, const TModChooser& chooser) {
    // Sort modes by name to keep the JSON deterministic; TModChooser stores them
    // in registration order, which would otherwise leak into the output.
    TVector<const TModChooser::TMode*> sortedModes;
    for (const auto* mode : chooser.GetUnsortedModes()) {
        if (mode->Hidden || mode->NoCompletion || mode->Name.empty()) {
            continue;
        }
        sortedModes.push_back(mode);
    }
    SortBy(sortedModes, [](const auto* m) { return m->Name; });
    json.WriteKey("handlers").BeginObject();
    for (const auto* mode : sortedModes) {
        json.WriteKey(mode->Name).BeginObject();
        WriteNodeBody(json, mode->Main);
        json.EndObject();
    }
    json.EndObject();
}

} // namespace

void GenerateJsonCompletion(const TModChooser& chooser, const TOpts& rootOpts, IOutputStream& out) {
    NJsonWriter::TBuf json(NJsonWriter::HEM_RELAXED, &out);
    json.BeginObject();
    WriteModes(json, chooser);
    WriteOpts(json, rootOpts);
    json.EndObject();
    out << "\n";
}

} // namespace NYdb::NConsoleClient
