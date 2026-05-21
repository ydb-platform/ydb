#include "completion_graph_json.h"

#include <library/cpp/json/writer/json.h>

#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

namespace {

using namespace NLastGetopt;

// Write the JSON value describing what the option accepts:
//   null         -- flag without a value (e.g. --help, --verbose)
//   [v1, v2]     -- option with a fixed set of allowed values (TOpt::Choices)
//   null         -- option that takes an arbitrary value (URL, path, number, ...)
//                   (no completion choices available)
void WriteOptValue(NJsonWriter::TBuf& json, const TOpt& opt) {
    if (opt.GetHasArg() == NO_ARGUMENT) {
        json.WriteNull();
        return;
    }
    // GetChoicesHelp() returns ", "-joined Choices_ set, the closest public
    // accessor available. Empty string means "no choices configured".
    const TString choicesHelp = opt.GetChoicesHelp();
    if (choicesHelp.empty()) {
        json.WriteNull();
        return;
    }
    TVector<TString> values;
    StringSplitter(choicesHelp).SplitByString(", ").SkipEmpty().Collect(&values);
    json.BeginList();
    for (const TString& value : values) {
        json.WriteString(StripString(value));
    }
    json.EndList();
}

void WriteOpts(NJsonWriter::TBuf& json, const TOpts& opts) {
    json.WriteKey("options").BeginObject();
    for (const TOpt* opt : opts.GetOpts()) {
        if (opt->IsHidden()) {
            continue;
        }
        for (const TString& name : opt->GetLongNames()) {
            json.WriteKey("--" + name);
            WriteOptValue(json, *opt);
        }
        for (char ch : opt->GetShortNames()) {
            char buf[2] = {'-', ch};
            json.WriteKey(TStringBuf(buf, sizeof(buf)));
            WriteOptValue(json, *opt);
        }
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
    json.WriteKey("handlers").BeginObject();
    for (const auto* mode : chooser.GetUnsortedModes()) {
        if (mode->Hidden || mode->NoCompletion || mode->Name.empty()) {
            continue;
        }
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
