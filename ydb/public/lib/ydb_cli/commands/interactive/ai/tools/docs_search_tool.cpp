#include "docs_search_tool.h"
#include "tool_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/log.h>

#include <library/cpp/archive/yarchive.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/yaml/as/tstring.h>
#include <yaml-cpp/yaml.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/memory/blob.h>
#include <util/stream/input.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

constexpr char DOCS_ARCHIVE_RESOURCE[] = "ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate/docs.archive";

class TDocStore {
public:
    explicit TDocStore(std::unordered_map<TString, TString> files)
        : Files(std::move(files))
    {}

    static std::shared_ptr<TDocStore> Load() {
        const TBlob blob = TBlob::FromStringSingleThreaded(NResource::Find(DOCS_ARCHIVE_RESOURCE));
        TArchiveReader reader(blob);

        std::unordered_map<TString, TString> files;
        files.reserve(reader.Count());

        size_t amountSize = 0;
        for (size_t i = 0; i < reader.Count(); ++i) {
            const TString key = reader.KeyByIndex(i);
            if (!key.EndsWith(".md") && !key.EndsWith(".yaml")) {
                continue;
            }

            const TBlob& payload = reader.ObjectBlobByKey(key);
            amountSize += payload.Size();
            YDB_CLI_LOG(Debug, "docs_search: loaded file \"" << key << "\" (" << payload.Size() << " bytes)");
            Y_VALIDATE(files.emplace(std::move(key), TString(payload.AsCharPtr(), payload.Size())).second, "Duplicate file in docs archive: \"" << key << "\"");
        }

        YDB_CLI_LOG(Info, "docs_search: loaded " << files.size() << " files from the archive, total size: " << amountSize << " bytes");
        return std::make_shared<TDocStore>(std::move(files));
    }

    const TString* Find(const TString& path) const {
        auto it = Files.find(path);
        return it != Files.end() ? &it->second : nullptr;
    }

private:
    const std::unordered_map<TString, TString> Files;
};

class TTocIndex {
public:
    struct TEntry {
        TString Path; // Path in archive, e.g. "core/concepts/architecture.md"
        std::vector<TString> Title; // Human-readable chain of section titles
    };

    TTocIndex(const TDocStore& store, const TString& root)
        : Root(root)
    {
        WalkTocFile(store, TStringBuilder() << Root << "/toc.yaml", root, {});
        std::sort(Entries.begin(), Entries.end(), [](const TEntry& l, const TEntry& r) {
            for (size_t i = 0; i < std::min(l.Title.size(), r.Title.size()); ++i) {
                if (l.Title[i] == r.Title[i]) {
                    continue;
                }
                return l.Title[i] < r.Title[i]; 
            }
            return l.Title.size() < r.Title.size();
        });
    }

    const std::vector<TEntry>& GetEntries() const{
        return Entries;
    }

    const TEntry* FindEntry(const TString& path) const {
        const auto it = EntriesIndex.find(path);
        if (it == EntriesIndex.end()) {
            return nullptr;
        }
        return &Entries[it->second];
    }

private:
    static TString JoinRelative(const TString& baseDir, const TString& rel) {
        if (rel.StartsWith('/')) {
            return rel.substr(1);
        }
        if (baseDir.empty()) {
            return TFsPath(rel).Fix().GetPath();
        }
        return TFsPath(baseDir).Child(rel).Fix().GetPath();
    }

    static TString DirectoryOf(const TString& path) {
        const auto slash = path.rfind('/');
        return slash == TString::npos ? "" : path.substr(0, slash);
    }

    static void AppendTitle(const TString& title, std::vector<TString>& titles) {
        if (titles.empty() || titles.back() != title) {
            titles.emplace_back(title);
        }
    }

    void WalkTocFile(const TDocStore& store, const TString& tocPath, const TString& baseDir, std::vector<TString> titles) {
        if (!VisitedTocs.insert(tocPath).second) {
            return;
        }

        const TString* content = store.Find(tocPath);
        if (!content) {
            YDB_CLI_LOG(Warning, "docs_search: TOC file \"" << tocPath << "\" was not found in the archive");
            return;
        }

        YAML::Node root;
        try {
            root = YAML::Load(*content);
        } catch (const std::exception& e) {
            YDB_CLI_LOG(Warning, "docs_search: failed to parse TOC file \"" << tocPath << "\": " << e.what());
            return;
        }

        if (const auto& title = root["title"]) {
            if (const auto& titleStr = title.as<TString>("")) {
                AppendTitle(titleStr, titles);
            }
        }

        WalkItems(store, root["items"], tocPath, baseDir, titles);
    }

    void WalkItems(const TDocStore& store, const YAML::Node& items, const TString& tocPath, const TString& baseDir, const std::vector<TString>& titles) {
        if (!items || !items.IsSequence()) {
            return;
        }

        for (const auto& item : items) {
            WalkItem(store, item, tocPath, baseDir, titles);
        }
    }

    void WalkItem(const TDocStore& store, const YAML::Node& item, const TString& tocPath, const TString& baseDir, const std::vector<TString>& titles) {
        if (!item || !item.IsMap()) {
            return;
        }

        std::vector<TString> nextTitles = titles;
        if (const auto& name = item["name"]) {
            if (const auto& nameStr = name.as<TString>("")) {
                AppendTitle(nameStr, nextTitles);
            }
        }

        if (const auto& href = item["href"]) {
            if (const auto& hrefStr = href.as<TString>(""); hrefStr && !hrefStr.StartsWith("http://") && !hrefStr.StartsWith("https://")) {
                if (const auto& resolved = JoinRelative(baseDir, hrefStr); resolved.EndsWith(".md") && store.Find(resolved) && SeenPages.insert(resolved).second) {
                    AddEntity(resolved, nextTitles);
                }
            }
        }

        if (const auto& childItems = item["items"]) {
            WalkItems(store, childItems, tocPath, baseDir, nextTitles);
        }

        if (const auto& include = item["include"]; include && include.IsMap()) {
            if (const auto& path = include["path"]) {
                if (const auto& pathStr = path.as<TString>("")) {
                    const TString mode = include["mode"] ? include["mode"].as<TString>("link") : TString("link");
                    const TString resolved = JoinRelative(baseDir, pathStr);
                    const TString includedBaseDir = mode == "merge" ? DirectoryOf(tocPath) : DirectoryOf(resolved);
                    YDB_CLI_LOG(Debug, "docs_search: including TOC file \"" << pathStr << "\" (mode \"" << mode << "\", base \"" << baseDir << "\")");
                    WalkTocFile(store, resolved, includedBaseDir, nextTitles);
                }
            }
        }
    }

    void AddEntity(const TString& path, const std::vector<TString>& title) {
        Y_VALIDATE(path.StartsWith(Root + "/"), "Unexpected entity path");
        const auto& canonizedPath = path.substr(Root.size() + 1);

        Y_VALIDATE(EntriesIndex.emplace(canonizedPath, Entries.size()).second, "Duplicated documentation page: " << path);
        Entries.push_back({canonizedPath, title});
    }

private:
    const TString Root;
    std::vector<TEntry> Entries;
    std::unordered_map<TString, ui64> EntriesIndex;
    std::unordered_set<TString> VisitedTocs;
    std::unordered_set<TString> SeenPages;
};

// TODO: support grep-like search
class TDocsSearchTool final : public TToolBase {
    using TBase = TToolBase;

    static constexpr char DESCRIPTION[] = R"(
Searches and retrieves YDB documentation pages from a bundled docs archive.

Use this tool to look up authoritative information about YDB concepts, SQL syntax,
configuration, deployment, and other topics described in the official documentation.

Supported actions:
- "list": returns the catalogue of all documentation pages. Each entry contains:
    * "path"  — archive-relative path to the page (e.g. "ru/core/concepts/architecture.md").
    * "title_path" — array of human-readable section titles leading to the page,
                        reconstructed from the documentation table of contents
                        (e.g. ["YDB", "Concepts", "Architecture"]).
    Use this action first to discover which pages exist before retrieving them.

- "get": returns the raw content of the page (Markdown or YAML) by its archive path.
    The "path" parameter is required for this action and must match a value returned
    by the "list" action.

You must provide "language" property with value matching the current conversation language.
)";

    static constexpr char ACTION_PROPERTY[] = "action";
    static constexpr char ACTION_LIST[] = "list";
    static constexpr char ACTION_GET[] = "get";

    static constexpr char LANG_PROPERTY[] = "language";
    static constexpr char LANG_RU[] = "ru";
    static constexpr char LANG_EN[] = "en";

    static constexpr char PATH_PROPERTY[] = "path";

public:
    TDocsSearchTool()
        : TBase(CreateParametersSchema(), DESCRIPTION)
    {}

protected:
    void ParseParameters(const NJson::TJsonValue& parameters) final {
        try {
            EnsureLoaded();
        } catch (const std::exception& e) {
            throw yexception() << "Failed to load docs archive: " << e.what();
        }

        TJsonParser parser(parameters);
        Action = to_lower(Strip(parser.GetKey(ACTION_PROPERTY).GetString()));
        if (Action != ACTION_LIST && Action != ACTION_GET) {
            throw yexception() << "Unknown action \"" << Action << "\". Expected \"" << ACTION_LIST << "\" or \"" << ACTION_GET << "\".";
        }

        Language = to_lower(Strip(parser.GetKey(LANG_PROPERTY).GetString()));
        if (Language != LANG_RU && Action != LANG_EN) {
            throw yexception() << "Unknown language \"" << Action << "\". Expected \"" << LANG_RU << "\" or \"" << LANG_EN << "\".";
        }

        Path.clear();
        std::vector<TString> pagePath;
        if (Action == ACTION_GET) {
            auto pathParser = parser.MaybeKey(PATH_PROPERTY);
            if (!pathParser) {
                throw yexception() << "\"" << PATH_PROPERTY << "\" parameter is required for action \"" << ACTION_GET << "\"";
            }

            Path = Strip(pathParser->GetString());
            if (Path.empty()) {
                throw yexception() << "\"" << PATH_PROPERTY << "\" must not be empty";
            }

            if (Path.StartsWith('/')) {
                Path = Path.substr(1);
            }

            const auto* entry = GetDocIndex().FindEntry(Path);
            if (!entry) {
                throw yexception() << "Documentation page \"" << Path << "\" was not found in the archive. Use action \"" << ACTION_LIST << "\" to discover available pages.";
            }
            pagePath = entry->Title;
        }

        const TString message = (Action == ACTION_LIST)
            ? TString("Listing YDB documentation pages...")
            : TStringBuilder() << "Reading documentation page \"" << JoinSeq('/', pagePath) << "\"";
        PrintFtxuiMessage("", message, ftxui::Color::Green);
    }

    bool AskPermissions() final {
        return true;
    }

    TResponse DoExecute() final {
        if (Action == ACTION_LIST) {
            return DoList();
        }
        if (Action == ACTION_GET) {
            return DoGet();
        }
        Y_VALIDATE(false, "Unknown action: " << Action);
    }

private:
    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Property(ACTION_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Enum({"list", "get"})
                .Description("Action to perform: \"list\" returns the catalogue of all documentation pages, \"get\" returns the content of a single page.")
                .Done()
            .Property(LANG_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Enum({"ru", "en"})
                .Description("Documentation language to select. Must be used same language as current conversation language.")
                .Done()
            .Property(PATH_PROPERTY, /* required */ false)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Archive-relative path to the documentation page (required when action is \"get\"). Use a value returned by the \"list\" action, e.g. \"ru/core/concepts/architecture.md\".")
                .Done()
            .Build();
    }

    void EnsureLoaded() {
        if (Store) {
            return;
        }

        Y_VALIDATE(NResource::Has(DOCS_ARCHIVE_RESOURCE), "Can not load docs, no docs.archive resource: " << DOCS_ARCHIVE_RESOURCE);

        Store = TDocStore::Load();
        RuDocIndex = std::make_shared<TTocIndex>(*Store, LANG_RU);
        EnDocIndex = std::make_shared<TTocIndex>(*Store, LANG_EN);
    }

    const TTocIndex& GetDocIndex() const {
        if (Language == LANG_RU) {
            return *RuDocIndex;
        }
        if (Language == LANG_EN) {
            return *EnDocIndex;
        }
        Y_VALIDATE(false, "Unknown documentation language: " << Language);
    }

    TResponse DoList() {
        NJson::TJsonValue result;
        ui64 payloadSize = 0;

        auto& array = result.SetType(NJson::JSON_ARRAY).GetArraySafe();
        const auto& entries = GetDocIndex().GetEntries();
        for (const auto& [path, title] : entries) {
            auto& item = array.emplace_back();

            item["path"] = path;
            payloadSize += path.size();

            auto& titles = item["title_path"].SetType(NJson::JSON_ARRAY).GetArraySafe();
            for (const auto& title : title) {
                titles.emplace_back(title);
                payloadSize += title.size();
            }
        }

        YDB_CLI_LOG(Info, "docs_search: listed " << array.size() << " pages in the archive, payload size: " << payloadSize);
        YDB_CLI_LOG(Debug, "docs_search: list result:\n" << FormatJsonValue(result));
        return TResponse::Success(std::move(result));
    }

    TResponse DoGet() {
        if (const TString* content = Store->Find(TStringBuilder() << Language << "/" << Path)) {
            YDB_CLI_LOG(Debug, "docs_search: extracted page with size " << content->size());
            YDB_CLI_LOG(Debug, "docs_search: get result:\n" << Strip(*content));
            return TResponse::Success(*content);
        }
        return TResponse::Error(TStringBuilder() << "Documentation page \"" << Path << "\" was not found in the archive. Use action \"" << ACTION_LIST << "\" to discover available pages.");
    }

private:
    std::shared_ptr<TDocStore> Store;
    std::shared_ptr<TTocIndex> RuDocIndex;
    std::shared_ptr<TTocIndex> EnDocIndex;
    TString Action;
    TString Language;
    TString Path;
};

} // anonymous namespace

ITool::TPtr CreateDocsSearchTool() {
    if (!NResource::Has(DOCS_ARCHIVE_RESOURCE)) {
        YDB_CLI_LOG(Notice, "Documentation search tool disabled, recompile with `-D YDB_CLI_AI_INCLUDE_DOCS` to enable tool")
        return nullptr;
    }
    return std::make_shared<TDocsSearchTool>();
}

} // namespace NYdb::NConsoleClient::NAi
