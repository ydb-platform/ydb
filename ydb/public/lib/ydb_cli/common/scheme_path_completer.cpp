#include "scheme_path_completer.h"

#include <library/cpp/getopt/small/last_getopt_opt.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYdb::NConsoleClient {

namespace {

constexpr TStringBuf SpecialFlag = "---CUSTOM-COMPLETION---";
constexpr TDuration CompletionTimeout = TDuration::Seconds(3);

struct TSchemeCompleterNameMapping {
    TStringBuf Name;
    ESchemePathKind Kind;
};

constexpr TSchemeCompleterNameMapping SchemeCompleterNames[] = {
    {"SchemePathCompleterTable", ESchemePathKind::Tables},
    {"SchemePathCompleterTopic", ESchemePathKind::Topics},
    {"SchemePathCompleterDir",   ESchemePathKind::Dir},
    {"SchemePathCompleterAll",   ESchemePathKind::All},
};

bool IsDirectoryLike(NScheme::ESchemeEntryType type) {
    return type == NScheme::ESchemeEntryType::Directory
        || type == NScheme::ESchemeEntryType::SubDomain
        || type == NScheme::ESchemeEntryType::ColumnStore
        || type == NScheme::ESchemeEntryType::BackupCollection;
}

bool IsAllowedType(NScheme::ESchemeEntryType type, ESchemePathKind kind) {
    switch (kind) {
        case ESchemePathKind::Tables:
            return type == NScheme::ESchemeEntryType::Table
                || type == NScheme::ESchemeEntryType::ColumnTable;
        case ESchemePathKind::Topics:
            return type == NScheme::ESchemeEntryType::Topic;
        case ESchemePathKind::Dir:
            return false;
        case ESchemePathKind::All:
            return !IsDirectoryLike(type);
    }
}

class TSchemePathCompleter : public NLastGetopt::NComp::TMultipartCustomCompleter {
public:
    TSchemePathCompleter(TStringBuf name)
        : TMultipartCustomCompleter("/")
        , UniqueName_(name)
    {
    }

    TStringBuf GetUniqueName() const override {
        return UniqueName_;
    }

    void GenerateCompletionParts(
        int /*argc*/, const char** /*argv*/,
        int /*curIdx*/, TStringBuf /*cur*/, TStringBuf /*prefix*/, TStringBuf /*suffix*/,
        TStringBuf /*root*/) override
    {
        // Actual completion is handled via Process override in TClientCommandRootCommon.
        // This method exists only because TMultipartCustomCompleter requires it.
    }

private:
    TString UniqueName_;
};

static TSchemePathCompleter SchemePathCompleterTableInst("SchemePathCompleterTable");
static NLastGetopt::NComp::TCustomCompleter::TReg RegTable(&SchemePathCompleterTableInst);

static TSchemePathCompleter SchemePathCompleterTopicInst("SchemePathCompleterTopic");
static NLastGetopt::NComp::TCustomCompleter::TReg RegTopic(&SchemePathCompleterTopicInst);

static TSchemePathCompleter SchemePathCompleterDirInst("SchemePathCompleterDir");
static NLastGetopt::NComp::TCustomCompleter::TReg RegDir(&SchemePathCompleterDirInst);

static TSchemePathCompleter SchemePathCompleterAllInst("SchemePathCompleterAll");
static NLastGetopt::NComp::TCustomCompleter::TReg RegAll(&SchemePathCompleterAllInst);

class TSchemePathLaunchSelfCompleter : public NLastGetopt::NComp::ICompleter {
public:
    TSchemePathLaunchSelfCompleter(NLastGetopt::NComp::TCustomCompleter* completer)
        : Completer_(completer)
    {
    }

    void GenerateBash(NLastGetopt::TFormattedOutput& out) const override {
        out.Line() << "local _scheme_lines";
        out.Line() << "mapfile -t _scheme_lines < <(${words[@]} " << SpecialFlag << " "
                    << Completer_->GetUniqueName()
                    << " \"${cword}\" \"\" \"\" 2> /dev/null)";
        out.Line() << "for _item in \"${_scheme_lines[@]:2}\"; do";
        out.Line() << "  [[ -z \"$_item\" ]] && continue";
        out.Line() << "  [[ \"$_item\" == \"$cur\"* ]] && COMPREPLY+=( \"$_item\" )";
        out.Line() << "done";
        out.Line() << "[[ ${#COMPREPLY[@]} -eq 1 && \"${COMPREPLY[0]}\" == */ ]] && need_space=\"\"";
    }

    TStringBuf GenerateZshAction(NLastGetopt::NComp::TCompleterManager& manager) const override {
        return manager.GetCompleterID(this);
    }

    void GenerateZsh(NLastGetopt::TFormattedOutput& out, NLastGetopt::NComp::TCompleterManager&) const override {
        out.Line() << "local items=( \"${(@f)$(${words_orig[@]} " << SpecialFlag << " "
                    << Completer_->GetUniqueName()
                    << " \"${current_orig}\" \"${prefix_orig}\" \"${suffix_orig}\" 2> /dev/null)}\" )";
        out.Line();
        out.Line() << "local rempat=${items[1]}";
        out.Line() << "shift items";
        out.Line();
        out.Line() << "local sep=${items[1]}";
        out.Line() << "shift items";
        out.Line();
        out.Line() << "local files=( ${items:#*\"${sep}\"} )";
        out.Line() << "local filenames=( ${files#\"${rempat}\"} )";
        out.Line() << "local dirs=( ${(M)items:#*\"${sep}\"} )";
        out.Line() << "local dirnames=( ${dirs#\"${rempat}\"} )";
        out.Line();
        out.Line() << "local need_suf";
        out.Line() << "compset -S \"${sep}*\" || need_suf=\"1\"";
        out.Line();
        out.Line() << "compadd ${@} ${expl[@]} -d filenames -- \"${(@)files}\"";
        out.Line() << "compadd ${@} ${expl[@]} ${need_suf:+-S\"${sep}\"} -q -d dirnames -- \"${(@)dirs%${sep}}\"";
    }

private:
    NLastGetopt::NComp::TCustomCompleter* Completer_;
};

} // anonymous namespace

NLastGetopt::NComp::ICompleterPtr SchemePathCompleterForTables() {
    return MakeSimpleShared<TSchemePathLaunchSelfCompleter>(&SchemePathCompleterTableInst);
}

NLastGetopt::NComp::ICompleterPtr SchemePathCompleterForTopics() {
    return MakeSimpleShared<TSchemePathLaunchSelfCompleter>(&SchemePathCompleterTopicInst);
}

NLastGetopt::NComp::ICompleterPtr SchemePathCompleterForDir() {
    return MakeSimpleShared<TSchemePathLaunchSelfCompleter>(&SchemePathCompleterDirInst);
}

NLastGetopt::NComp::ICompleterPtr SchemePathCompleterForAll() {
    return MakeSimpleShared<TSchemePathLaunchSelfCompleter>(&SchemePathCompleterAllInst);
}

std::optional<TSchemeCompletionContext> DetectSchemeCompletion(int& argc, const char** argv) {
    for (int i = 1; i < argc - 4; ++i) {
        if (SpecialFlag != argv[i]) {
            continue;
        }
        TStringBuf name(argv[i + 1]);
        for (const auto& mapping : SchemeCompleterNames) {
            if (mapping.Name == name) {
                auto curIdx = FromString<int>(argv[i + 2]);
                auto prefix = TStringBuf(argv[i + 3]);
                auto suffix = TStringBuf(argv[i + 4]);

                if (!prefix && !suffix && 0 <= curIdx && curIdx < i) {
                    prefix = TStringBuf(argv[curIdx]);
                }

                TSchemeCompletionContext ctx;
                ctx.Kind = mapping.Kind;
                ctx.Prefix = TString(prefix);
                ctx.Suffix = TString(suffix);
                argc = i;
                return ctx;
            }
        }
        return std::nullopt;
    }
    return std::nullopt;
}

void RunSchemeCompletion(
    TDriver& driver,
    const TString& database,
    const TSchemeCompletionContext& ctx)
{
    TStringBuf prefix(ctx.Prefix);
    TStringBuf root;
    if (prefix.Contains('/')) {
        TStringBuf tmp;
        prefix.RSplit('/', root, tmp);
    }

    if (root) {
        Cout << root << "/" << Endl;
    } else {
        Cout << Endl;
    }
    Cout << "/" << Endl;

    NScheme::TSchemeClient client(driver);

    TString listPath;
    if (root && TStringBuf(root).StartsWith('/')) {
        listPath = TString(root);
    } else if (root) {
        listPath = database + "/" + root;
    } else {
        listPath = database;
    }

    auto settings = NScheme::TListDirectorySettings();
    settings.ClientTimeout(CompletionTimeout);

    auto result = client.ListDirectory(listPath, settings).GetValueSync();
    if (!result.IsSuccess()) {
        return;
    }

    for (const auto& child : result.GetChildren()) {
        bool isDirLike = IsDirectoryLike(child.Type);

        if (!isDirLike && !IsAllowedType(child.Type, ctx.Kind)) {
            continue;
        }

        TString completion;
        if (root) {
            completion = TString(root) + "/" + child.Name;
        } else {
            completion = child.Name;
        }

        if (isDirLike) {
            completion += "/";
        }

        Cout << completion << Endl;
    }
}

void SetSchemePathCompletionForTables(NLastGetopt::TFreeArgSpec& spec) {
    spec.CompletionArgHelp("<database path>").Completer(SchemePathCompleterForTables());
}

void SetSchemePathCompletionForTopics(NLastGetopt::TFreeArgSpec& spec) {
    spec.CompletionArgHelp("<database path>").Completer(SchemePathCompleterForTopics());
}

void SetSchemePathCompletionForDir(NLastGetopt::TFreeArgSpec& spec) {
    spec.CompletionArgHelp("<database path>").Completer(SchemePathCompleterForDir());
}

void SetSchemePathCompletionForAll(NLastGetopt::TFreeArgSpec& spec) {
    spec.CompletionArgHelp("<database path>").Completer(SchemePathCompleterForAll());
}

} // namespace NYdb::NConsoleClient
