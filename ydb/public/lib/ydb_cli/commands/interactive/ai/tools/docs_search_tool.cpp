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
    using TPtr = std::shared_ptr<const TDocStore>;

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
            TString key = reader.KeyByIndex(i);
            if (!key.EndsWith(".md") && !key.EndsWith(".yaml")) {
                continue;
            }

            const TBlob& payload = reader.ObjectBlobByKey(key);
            amountSize += payload.Size();
            YDB_CLI_LOG(Debug, "Loaded documentation file \"" << key << "\" (" << payload.Size() << " bytes)");

            if (auto payloadStr = Strip(TString(payload.AsCharPtr(), payload.Size()))) {
                Y_VALIDATE(files.emplace(std::move(key), std::move(payloadStr)).second, "Duplicate file in docs archive: \"" << key << "\"");
            }
        }

        YDB_CLI_LOG(Info, "Loaded " << files.size() << " documentation files from the archive, total size: " << amountSize << " bytes");
        return std::make_shared<TDocStore>(std::move(files));
    }

    const TString* Find(const TString& path) const {
        auto it = Files.find(path);
        return it != Files.end() ? &it->second : nullptr;
    }

private:
    const std::unordered_map<TString, TString> Files;
};

class TTemplateResolver {
    // Block in documentation: {% <block name> arg1 arg2 ... %}
    static constexpr TStringBuf BLOCK_CUT = "cut";
    static constexpr TStringBuf BLOCK_ENDCUT = "endcut";

    static constexpr TStringBuf BLOCK_NOTE = "note";
    static constexpr TStringBuf BLOCK_ENDNOTE = "endnote";

    static constexpr TStringBuf BLOCK_BLOCK = "block";
    static constexpr TStringBuf BLOCK_ENDBLOCK = "endblock";

    static constexpr TStringBuf BLOCK_LIST = "list";
    static constexpr TStringBuf BLOCK_ENDLIST = "endlist";

    static constexpr TStringBuf BLOCK_IF = "if";
    static constexpr TStringBuf BLOCK_ELSE = "else";
    static constexpr TStringBuf BLOCK_ENDIF = "endif";

    static constexpr TStringBuf BLOCK_INCLUDE = "include";
    static constexpr TStringBuf BLOCK_FILE = "file";

public:
    using TPtr = std::shared_ptr<TTemplateResolver>;

    TString Resolve(const TString& text) {
        for (size_t i = text.find_first_of("{`"); i != TString::npos; i = text.find_first_of("{`", i + 1)) {
            if (text[i] == '`') {
                // Skip markdown blocks
                if (i + 2 < text.size() && text[i + 1] == '`' && text[i + 2] == '`') {
                    i = text.find("```", i + 3);

                    if (i != TString::npos) {
                        i += 2;
                    } else {
                        i = text.size();
                    }
                }
                continue;
            }

            if (i + 1 == text.size() || !IsIn({'%', '{', '#'}, text[i + 1])) {
                continue;
            }

            if (text[i + 1] == '#') {
                if (i + 3 >= text.size() || text[i + 2] != 'T' || text[i + 3] != '}') {
                    // Paragraph anchor definition, skip
                    continue;
                }

                // TODO: support statement {#T}
                continue;
            }

            if (text[i + 1] == '%' && i + 2 < text.size() && text[i + 2] == ' ') {
                if (ContainsBlock(text, i + 3, BLOCK_CUT) || ContainsBlock(text, i + 3, BLOCK_ENDCUT)) {
                    // Cut block, skip
                    continue;
                }

                if (ContainsBlock(text, i + 3, BLOCK_NOTE) || ContainsBlock(text, i + 3, BLOCK_ENDNOTE)) {
                    // Notice block, skip
                    continue;
                }

                if (ContainsBlock(text, i + 3, BLOCK_BLOCK) || ContainsBlock(text, i + 3, BLOCK_ENDBLOCK)) {
                    // Simple block, skip
                    continue;
                }

                if (ContainsBlock(text, i + 3, BLOCK_LIST) || ContainsBlock(text, i + 3, BLOCK_ENDLIST)) {
                    // Inline documentation blocks, skip
                    continue;
                }

                if (ContainsBlock(text, i + 3, BLOCK_IF) || ContainsBlock(text, i + 3, BLOCK_ELSE) || ContainsBlock(text, i + 3, BLOCK_ENDIF)) {
                    // TODO: support if
                    continue;
                }

                if (ContainsBlock(text, i + 3, BLOCK_INCLUDE) || ContainsBlock(text, i + 3, BLOCK_FILE)) {
                    // TODO: handle include/file block
                    continue;
                }
            }

            if (text[i + 1] == '{') {
                if (const auto close = text.find("}}"); close != TString::npos) {
                    // TODO: support variables
                    continue;
                }
            }

            // Report unknown template
            const TString close = text[i + 1] == '{' ? "}}" : "}";
            const auto start = i;
            i = text.find(close, i + 2);
            if (i == TString::npos) {
                i = text.size();
            }
            UnknownTemplates.emplace(text.substr(start, std::min(i + close.size() - start, text.size() - start)));
        }

        return text;
    }

    void Finish() {
        if (UnknownTemplates.empty()) {
            return;
        }

        YDB_CLI_LOG(Notice, "Found " << UnknownTemplates.size() << " different unknown documentation templates");

        if (auto entry = GetGlobalLogger().Info(); entry.LogEnabled()) {
            entry << "Unknown documentation templates:";
            for (const auto& t : UnknownTemplates) {
                entry << "\n" << t;
            }
        }
    }

private:
    static bool ContainsBlock(const TString& text, size_t startPos, TStringBuf block) {
        return startPos + block.size() <= text.size() && text.substr(startPos, block.size()) == block;
    }

    std::unordered_set<TString> UnknownTemplates;
};

class TTocIndex {
public:
    using TPtr = std::shared_ptr<TTocIndex>;

    struct TEntry {
        TString Path; // Path in archive, e.g. "core/concepts/architecture.md"
        TString Content; // Resolved page content after templates substitution
        std::vector<TString> Title; // Human-readable chain of section titles
    };

    TTocIndex(TDocStore::TPtr store, TTemplateResolver::TPtr templateResolver, const TString& root)
        : Root(root)
        , Store(std::move(store))
        , TemplateResolver(std::move(templateResolver))
    {
        WalkTocFile(TStringBuilder() << Root << "/toc.yaml", root, {});

        std::sort(Entries.begin(), Entries.end(), [](const TEntry& l, const TEntry& r) {
            for (size_t i = 0; i < std::min(l.Title.size(), r.Title.size()); ++i) {
                if (l.Title[i] == r.Title[i]) {
                    continue;
                }
                return l.Title[i] < r.Title[i]; 
            }
            return l.Title.size() < r.Title.size();
        });

        EntriesIndex.reserve(Entries.size());
        for (ui64 i = 0; i < Entries.size(); ++i) {
            Y_VALIDATE(EntriesIndex.emplace(Entries[i].Path, i).second, "Duplicated documentation page: " << Entries[i].Path);
        }
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

    void AppendTitle(const TString& title, std::vector<TString>& titles) const {
        if (titles.empty() || titles.back() != title) {
            titles.emplace_back(TemplateResolver->Resolve(title));
        }
    }

    void WalkTocFile(const TString& tocPath, const TString& baseDir, std::vector<TString> titles) {
        if (!VisitedTocs.insert(tocPath).second) {
            return;
        }

        const TString* content = Store->Find(tocPath);
        if (!content) {
            YDB_CLI_LOG(Warning, "Documentation TOC file \"" << tocPath << "\" was not found in the archive");
            return;
        }

        YAML::Node root;
        try {
            root = YAML::Load(*content);
        } catch (const std::exception& e) {
            YDB_CLI_LOG(Warning, "Failed to parse documentation TOC file \"" << tocPath << "\": " << e.what());
            return;
        }

        if (const auto& title = root["title"]) {
            if (const auto& titleStr = title.as<TString>("")) {
                AppendTitle(titleStr, titles);
            }
        }

        WalkItems(root["items"], tocPath, baseDir, titles);
    }

    void WalkItems(const YAML::Node& items, const TString& tocPath, const TString& baseDir, const std::vector<TString>& titles) {
        if (!items || !items.IsSequence()) {
            return;
        }

        for (const auto& item : items) {
            WalkItem(item, tocPath, baseDir, titles);
        }
    }

    void WalkItem(const YAML::Node& item, const TString& tocPath, const TString& baseDir, const std::vector<TString>& titles) {
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
                if (const auto& resolved = JoinRelative(baseDir, hrefStr); resolved.EndsWith(".md")) {
                    if (const auto* rawContent = Store->Find(resolved); rawContent && SeenPages.insert(resolved).second) {
                        AddEntity(resolved, nextTitles, *rawContent);
                    }
                }
            }
        }

        if (const auto& childItems = item["items"]) {
            WalkItems(childItems, tocPath, baseDir, nextTitles);
        }

        if (const auto& include = item["include"]; include && include.IsMap()) {
            if (const auto& path = include["path"]) {
                if (const auto& pathStr = path.as<TString>("")) {
                    const TString mode = include["mode"] ? include["mode"].as<TString>("link") : TString("link");
                    const TString resolved = JoinRelative(baseDir, pathStr);
                    const TString includedBaseDir = mode == "merge" ? DirectoryOf(tocPath) : DirectoryOf(resolved);
                    YDB_CLI_LOG(Debug, "Including documentation TOC file \"" << pathStr << "\" (mode \"" << mode << "\", base \"" << baseDir << "\")");
                    WalkTocFile(resolved, includedBaseDir, nextTitles);
                }
            }
        }
    }

    void AddEntity(const TString& path, const std::vector<TString>& title, const TString& rawContent) {
        Y_VALIDATE(path.StartsWith(Root + "/"), "Unexpected entity path");
        Entries.push_back({
            .Path = path.substr(Root.size() + 1),
            .Content = TemplateResolver->Resolve(rawContent),
            .Title = title,
        });
    }

private:
    const TString Root;
    const TDocStore::TPtr Store;
    const TTemplateResolver::TPtr TemplateResolver;
    std::vector<TEntry> Entries;
    std::unordered_map<TString, ui64> EntriesIndex;
    std::unordered_set<TString> VisitedTocs;
    std::unordered_set<TString> SeenPages;
};

// TODO: support grep-like search
// TODO: resolve documentation templates
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
            : TStringBuilder() << "Reading documentation page " << JoinSeq('/', pagePath);
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

        const auto templateResolver = std::make_shared<TTemplateResolver>();
        RuDocIndex = std::make_shared<TTocIndex>(Store, templateResolver, LANG_RU);
        EnDocIndex = std::make_shared<TTocIndex>(Store, templateResolver, LANG_EN);
        templateResolver->Finish();
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
        for (const auto& entry : entries) {
            auto& item = array.emplace_back();

            item["path"] = entry.Path;
            payloadSize += entry.Path.size();

            auto& titles = item["title_path"].SetType(NJson::JSON_ARRAY).GetArraySafe();
            for (const auto& title : entry.Title) {
                titles.emplace_back(title);
                payloadSize += title.size();
            }
        }

        YDB_CLI_LOG(Info, "Listed " << array.size() << " documentation pages in the archive, payload size: " << payloadSize);
        YDB_CLI_LOG(Debug, "Full documentation list result:\n" << FormatJsonValue(result));
        return TResponse::Success(std::move(result));
    }

    TResponse DoGet() {
        if (const auto* entry = GetDocIndex().FindEntry(Path)) {
            YDB_CLI_LOG(Info, "Extracted documentation page (size " << entry->Content.size() << "):\n" << Strip(entry->Content));
            return TResponse::Success(entry->Content);
        }
        return TResponse::Error(TStringBuilder() << "Documentation page \"" << Path << "\" was not found in the archive. Use action \"" << ACTION_LIST << "\" to discover available pages.");
    }

private:
    TDocStore::TPtr Store;
    TTocIndex::TPtr RuDocIndex;
    TTocIndex::TPtr EnDocIndex;
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
