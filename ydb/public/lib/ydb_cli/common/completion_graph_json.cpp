#include "completion_graph_json.h"

#include <library/cpp/getopt/small/completer.h>
#include <library/cpp/json/writer/json.h>

#include <util/generic/algorithm.h>
#include <util/string/split.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

namespace {

using namespace NLastGetopt;

// Markers for a free-form value (no fixed set of choices):
//   "file"  -- the value is completed as a local filesystem path,
//   "value" -- the value has custom/server-side completion (e.g. a YDB scheme
//              path) that a third party cannot reproduce as local files.
constexpr TStringBuf FileArgMarker = "file";
constexpr TStringBuf ValueArgMarker = "value";

// Decide whether a value with the given completer (or no completer at all) is
// completed as a local file/path. This mirrors what the native zsh generator
// does: an option/argument without a completer falls back to `_default`, and the
// File()/Directory()/Default() completers emit `_files`/`_default` -- all of
// which list local filesystem entries. Every other completer (host, pid,
// choices, scheme path, ...) completes something a third party cannot turn into
// local files. We probe the completer through the zsh action it generates,
// which is the only public way to introspect the otherwise opaque ICompleter.
bool CompletesLocalFiles(const NComp::ICompleterPtr& completer) {
    if (!completer) {
        return true;
    }
    NComp::TCompleterManager manager{"ydb"};
    const TStringBuf action = completer->GenerateZshAction(manager);
    return action.StartsWith("_files") || action.StartsWith("_default");
}

// Write the option's allowed values as a sorted JSON list. TOpt::Choices_ is a
// THashSet<TString> exposed only via GetChoicesHelp() as a ", "-joined string.
// Choices in YDB CLI are plain identifiers (e.g. "csv", "json"), so splitting by
// ", " is safe in practice. Output is sorted to keep the JSON deterministic.
void WriteChoices(NJsonWriter::TBuf& json, const TString& choicesHelp) {
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

// Write the JSON value describing what the option accepts:
//   null         -- a flag without a value (e.g. --help, --verbose),
//   [v1, v2]     -- a fixed set of allowed values (TOpt::Choices), sorted,
//   "file"       -- a free-form value completed as a local file/path,
//   "value"      -- a free-form value with custom/server-side completion.
void WriteOptValue(NJsonWriter::TBuf& json, const TOpt& opt) {
    if (opt.GetHasArg() == NO_ARGUMENT) {
        json.WriteNull();
        return;
    }
    const TString choicesHelp = opt.GetChoicesHelp();
    if (!choicesHelp.empty()) {
        WriteChoices(json, choicesHelp);
        return;
    }
    json.WriteString(CompletesLocalFiles(opt.Completer_) ? FileArgMarker : ValueArgMarker);
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

// Write the JSON value describing the positional (free) arguments a leaf command
// accepts:
//   null     -- no positional arguments. Also used when the command left the
//               default "unlimited but undescribed" policy untouched: we must
//               not claim file completion there, otherwise commands that simply
//               never declared positionals (e.g. `table query execute`) would
//               make a third party dump the current directory.
//   "file"   -- positional(s) completed as local files/paths,
//   "value"  -- positional(s) with custom/server-side completion (e.g. a YDB
//               scheme path).
void WriteFreeArgsValue(NJsonWriter::TBuf& json, const TOpts& opts) {
    const ui32 maxArgs = opts.GetFreeArgsMax();
    // TFreeArgSpec::IsDefault() inspects only Title_/Help_, not the completer, so
    // check Completer_ explicitly: a trailing completer set without a title/help
    // is a declared positional and must not be mistaken for the untouched default.
    const bool untouchedDefault =
        maxArgs == TOpts::UNLIMITED_ARGS &&
        opts.GetFreeArgsMin() == 0 &&
        opts.GetFreeArgSpecs().empty() &&
        opts.GetTrailingArgSpec().IsDefault() &&
        !opts.GetTrailingArgSpec().Completer_;
    if (maxArgs == 0 || untouchedDefault) {
        json.WriteNull();
        return;
    }
    // Report "file" only if every declared positional (and the trailing one,
    // when the count is unbounded) completes as a local file/path; otherwise
    // some slot uses custom completion and we fall back to the opaque "value".
    bool files = true;
    for (const auto& spec : opts.GetFreeArgSpecs()) {
        if (!CompletesLocalFiles(spec.second.Completer_)) {
            files = false;
            break;
        }
    }
    if (files && maxArgs == TOpts::UNLIMITED_ARGS && !CompletesLocalFiles(opts.GetTrailingArgSpec().Completer_)) {
        files = false;
    }
    json.WriteString(files ? FileArgMarker : ValueArgMarker);
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
        const TOpts& opts = mainArgs->GetOptions();
        WriteOpts(json, opts);
        // Positional arguments are meaningful only on leaves. On a command group
        // (mainModes) the first positional selects a subcommand, which is already
        // described by "handlers", so report no free args there.
        json.WriteKey("free_args");
        if (mainModes) {
            json.WriteNull();
        } else {
            WriteFreeArgsValue(json, opts);
        }
    } else {
        json.WriteKey("options").BeginObject().EndObject();
        json.WriteKey("free_args").WriteNull();
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
    // The root acts as a command group: its positional selects a subcommand.
    json.WriteKey("free_args").WriteNull();
    json.EndObject();
    out << "\n";
}

} // namespace NYdb::NConsoleClient
