#include "docs_search_tool.h"
#include "tool_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/common/utf8_utils.h>

#include <library/cpp/archive/yarchive.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/yaml/as/tstring.h>
#include <yaml-cpp/yaml.h>

#include <util/folder/path.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/yexception.h>
#include <util/memory/blob.h>
#include <util/stream/input.h>
#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

constexpr char DOCS_ARCHIVE_RESOURCE[] = "ydb/public/lib/ydb_cli/commands/interactive/ai/tools/docs_generate/docs.archive";

TString JoinRelative(const TString& baseDir, const TString& rel, ui64 protectRoot = 0) {
    if (rel.StartsWith('/')) {
        return rel.substr(1);
    }

    if (baseDir.empty()) {
        return TFsPath(rel).Fix().GetPath();
    }

    // Disable up's more than protectRoot folders

    TString dir = baseDir;
    TStringBuilder root;
    for (; protectRoot; --protectRoot) {
        const auto childPos = dir.find('/');
        if (childPos == TString::npos) {
            break;
        }
        root << dir.substr(0, childPos) << '/';
        dir = dir.substr(childPos + 1);
    }

    const auto path = TFsPath(dir).Child(rel).Fix();
    TStringBuf pathBuffer = path.GetPath();
    while (pathBuffer.SkipPrefix("../") || pathBuffer.SkipPrefix("./")) {}

    return root << pathBuffer;
}

TString DirectoryOf(const TString& path) {
    const auto slash = path.rfind('/');
    return slash == TString::npos ? "" : path.substr(0, slash);
}

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
            Y_VALIDATE(files.emplace(std::move(key), Strip(TString(payload.AsCharPtr(), payload.Size()))).second, "Duplicate file in docs archive: \"" << key << "\"");
        }

        YDB_CLI_LOG(Info, "Loaded " << files.size() << " documentation files from the archive, total size: " << amountSize << " bytes");
        return std::make_shared<TDocStore>(std::move(files));
    }

    const TString* Find(const TString& path) const {
        auto it = Files.find(path);
        return it != Files.end() ? &it->second : nullptr;
    }

    const std::unordered_map<TString, TString>& GetFiles() const {
        return Files;
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

    // File with documentation variables definition
    static constexpr TStringBuf PRESETS_FILE = "presets.yaml";

    struct TReplace {
        ui64 From = 0;
        ui64 To = 0;
        TString Content;
    };

public:
    using TPtr = std::shared_ptr<TTemplateResolver>;

    explicit TTemplateResolver(TDocStore::TPtr store)
        : Store(std::move(store))
    {
        for (const auto& [path, content] : Store->GetFiles()) {
            if (path != PRESETS_FILE && !path.EndsWith(TStringBuilder() << "/" << PRESETS_FILE)) {
                continue;
            }

            YAML::Node root;
            try {
                root = YAML::Load(content);
            } catch (const std::exception& e) {
                YDB_CLI_LOG(Warning, "Failed to parse documentation presets.yaml file: " << e.what());
                continue;
            }

            ParsePresetsFile(root["default"], DirectoryOf(path));
            ParsePresetsFile(root["ydb"], DirectoryOf(path));
        }
    }

    TString Resolve(const TString& text, const TString& baseDir) {
        const auto [it, inserted] = ParsedPages.emplace(std::pair(baseDir, text), "");
        if (!inserted) {
            return it->second;
        }

        std::vector<TReplace> replaces;
        for (size_t i = text.find_first_of("{`"); i != TString::npos; i = text.find_first_of("{`", i + 1)) {
            if (text[i] == '`') {
                if (i + 2 < text.size() && text[i + 1] == '`' && text[i + 2] == '`') {
                    // Skip markdown blocks
                    i = text.find("```", i + 3);

                    if (i != TString::npos) {
                        i += 2;
                    } else {
                        i = text.size();
                    }
                } else {
                    // Skip markdown comments
                    i = std::min(text.find("`", i + 1), text.size());
                }
                continue;
            }

            if (i + 1 == text.size() || !IsIn({'%', '{', '#'}, text[i + 1])) {
                continue;
            }

            if (text[i + 1] == '#') {
                // Paragraph anchor definition, skip
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

                if (ContainsBlock(text, i + 3, BLOCK_FILE)) {
                    // File block, skip
                    continue;
                }

                if (ContainsBlock(text, i + 3, BLOCK_IF) || ContainsBlock(text, i + 3, BLOCK_ELSE) || ContainsBlock(text, i + 3, BLOCK_ENDIF)) {
                    continue;
                }

                if (ResolveInclude(text, baseDir, i, replaces)) {
                    continue;
                }
            }

            if (text[i + 1] == '{' && ResolveVariable(text, baseDir, i, replaces)) {
                continue;
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

        TStringBuilder result;
        ui64 pos = 0;
        for (const auto& replace : replaces) {
            Y_VALIDATE(replace.From >= pos, "Unexpected replace start: " << replace.From << ", should be >= " << pos);
            result << text.substr(pos, replace.From - pos) << replace.Content;

            Y_VALIDATE(replace.From < replace.To, "Invalid replace range [" << replace.From << "; " << replace.To << ")");
            pos = replace.To;
        }

        if (pos < text.size()) {
            result << text.substr(pos);
        }

        it->second = result;
        return it->second;
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

    static TString GetBlockContext(const TString& text, size_t startPos, char right = '}') {
        return text.substr(startPos, std::min(text.find(right, startPos + 1), text.size() - 1) - startPos + 1);
    }

    void ParsePresetsFile(const YAML::Node& d, const TString& directory, const TString& prefix = "") {
        if (!d || !d.IsMap()) {
            return;
        }

        for (YAML::const_iterator it = d.begin(); it != d.end(); ++it) {
            const auto& key = it->first;
            const auto& value = it->second;
            if (!key || !value) {
                continue;
            }

            auto keyStr = key.as<TString>("");
            if (!keyStr) {
                continue;
            }
            keyStr = TStringBuilder() << prefix << keyStr;

            if (value.IsMap()) {
                ParsePresetsFile(value, directory, TStringBuilder() << keyStr << ".");
            }

            auto valueStr = value.as<TString>("");
            if (!valueStr) {
                continue;
            }

            const auto varIt = PresetVariables.emplace(keyStr, std::unordered_map<TString, TString>{}).first;
            if (!varIt->second.emplace(directory, std::move(valueStr)).second) {
                YDB_CLI_LOG(Warning, "Duplicated presets.yaml variable: " << keyStr << " definition in directory " << directory);
            }
        }
    }

    std::optional<TString> GetVariableValue(const TString& name, const TString& baseDir) const {
        const auto it = PresetVariables.find(name);
        if (it == PresetVariables.end()) {
            return std::nullopt;
        }

        TString resultValue;
        std::optional<TString> resultDirectory;
        for (const auto& [directory, value] : it->second) {
            if (!baseDir.StartsWith(directory)) {
                continue;
            }

            if (!resultDirectory || resultDirectory->size() < directory.size()) {
                resultDirectory = directory;
                resultValue = value;
            } else if (resultDirectory->size() == directory.size()) {
                YDB_CLI_LOG(Info, "Found duplicated variable instances from same directory prefix: " << directory);
            }
        }

        return resultValue;
    }

    const TString* GetFileValue(const TString& text, const TString& baseDir, size_t startPos, size_t endPos = TString::npos, TString* path = nullptr) {
        const auto fileRefBegin = text.find_first_of("(\"", startPos);
        if (fileRefBegin >= endPos) {
            return nullptr;
        }

        const auto fileRefEnd = text.find_first_of("\"#)", fileRefBegin + 1);
        if (fileRefEnd >= endPos || fileRefEnd - fileRefBegin <= 1) {
            return nullptr;
        }

        const auto& filePath = JoinRelative(baseDir, text.substr(fileRefBegin + 1, fileRefEnd - fileRefBegin - 1), /* protectRoot */ 2);
        if (path) {
            *path = filePath;
        }

        return Store->Find(filePath);
    }

    bool ResolveInclude(const TString& text, const TString& baseDir, size_t& startPos, std::vector<TReplace>& replaces) {
        if (!ContainsBlock(text, startPos + 3, BLOCK_INCLUDE)) {
            return false;
        }

        const auto includeBlockEnd = text.find("%}", startPos + 3 + BLOCK_INCLUDE.size());
        if (includeBlockEnd == TString::npos) {
            YDB_CLI_LOG(Info, "Failed to find include block end around: " << GetBlockContext(text, startPos));
            return false;
        }

        TString includePath;
        const auto* rawContent = GetFileValue(text, baseDir, startPos + 3 + BLOCK_INCLUDE.size(), includeBlockEnd, &includePath);
        if (!rawContent) {
            YDB_CLI_LOG(Info, "Failed to find include file under base dir '" << baseDir << "' (tried path: '" << includePath << "') around: " << GetBlockContext(text, startPos));
            return false;
        }

        replaces.push_back({
            .From = startPos,
            .To = includeBlockEnd + 2,
            .Content = Resolve(*rawContent, DirectoryOf(includePath)),
        });
        startPos = includeBlockEnd + 1;

        return true;
    }

    bool ResolveVariable(const TString& text, const TString& baseDir, size_t& startPos, std::vector<TReplace>& replaces) const {
        if (startPos + 1 >= text.size() || text[startPos] != '{' || text[startPos + 1] != '{') {
            return false;
        }

        const auto variableEnd = text.find("}}", startPos + 2);
        if (variableEnd == TString::npos) {
            YDB_CLI_LOG(Info, "Failed to find variable end around: " << GetBlockContext(text, startPos));
            return false;
        }

        const TString variableName = Strip(text.substr(startPos + 2, variableEnd - startPos - 2));
        const auto& variableValue = GetVariableValue(variableName, baseDir);
        if (!variableValue) {
            YDB_CLI_LOG(Info, "Failed to find variable '" << variableName << "' in presets.yaml around: " << GetBlockContext(text, startPos));
            return false;
        }

        replaces.push_back({
            .From = startPos,
            .To = variableEnd + 2,
            .Content = *variableValue,
        });
        startPos = variableEnd + 1;

        return true;
    }

    const TDocStore::TPtr Store;
    THashMap<std::pair<TString, TString>, TString> ParsedPages; // {(BaseDir, RawContent) -> ResolvedContent}
    std::unordered_map<TString, std::unordered_map<TString, TString>> PresetVariables; // {Name -> {Preset Directory -> Value}}
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

    std::vector<TString> PathToTitles(TString& path) const {
        while (path) {
            const auto it = DirectoriesNames.find(path);
            if (it != DirectoriesNames.end()) {
                return it->second;
            }

            path = DirectoryOf(path);
        }

        return {};
    }

private:
    void AppendTitle(const TString& title, const TString& tocPath, std::vector<TString>& titles) {
        if (const auto& dir = DirectoryOf(CanonizePath(tocPath)); DirectoriesNames.emplace(dir, titles).second) {
            YDB_CLI_LOG(Debug, "Added directory '" << dir << "'name '" << JoinSeq('/', titles) << "'");
        }

        if (titles.empty() || titles.back() != title) {
            titles.emplace_back(TemplateResolver->Resolve(title, DirectoryOf(tocPath)));
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
                AppendTitle(nameStr, tocPath, nextTitles);
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
        Entries.push_back({
            .Path = CanonizePath(path),
            .Content = TemplateResolver->Resolve(rawContent, DirectoryOf(path)),
            .Title = title,
        });
    }

    TString CanonizePath(const TString& path) const {
        Y_VALIDATE(path.StartsWith(Root + "/"), "Unexpected entity path: '" << path << "', should start with '" << Root << "/'");
        return path.substr(Root.size() + 1);
    }

private:
    const TString Root;
    const TDocStore::TPtr Store;
    const TTemplateResolver::TPtr TemplateResolver;
    std::vector<TEntry> Entries;
    std::unordered_map<TString, ui64> EntriesIndex;
    std::unordered_map<TString, std::vector<TString>> DirectoriesNames;
    std::unordered_set<TString> VisitedTocs;
    std::unordered_set<TString> SeenPages;
};

class TDocsSearchTool final : public TToolBase {
    using TBase = TToolBase;

    static constexpr char DESCRIPTION[] = R"(
Searches and retrieves YDB documentation pages from a bundled docs archive.

Use this tool to look up authoritative information about YDB concepts, YQL syntax,
configuration, deployment, and other topics described in the official documentation.

The returned pages are raw Markdown/YAML. Do not paste large fragments verbatim: extract the relevant facts and
re-express them following the system prompt's OUTPUT FORMATTING rules (plain text, with language-tagged fenced code
blocks for code or queries and Markdown tables for tabular data).

Supported actions:
- "list": returns the catalog of all documentation pages. Each entry contains:
    * "path" — archive-relative path to the page (e.g., "core/concepts/architecture.md").
    * "title_path" — array of human-readable section titles leading to the page,
                        reconstructed from the documentation table of contents
                        (e.g., ["YDB", "Concepts", "Architecture"]).
    Use this action first to discover which pages exist before retrieving them.
    The optional "path" parameter may be used to provide an archive-relative path prefix for pages that should be listed;
    it must match a value returned from the "list" action for the parent or root directory.

- "get": returns the raw content of the page (Markdown or YAML) by its archive path.
    The "path" parameter is required for this action and must match a value returned
    by the "list" action.

- "find": returns all occurrences of the "pattern" parameter with an exact case-sensitive match and surrounding context.
    Use this action when you are not sure which page you should read but have a pattern to search for.
    The optional "path" parameter may be used to select the folder where the search should be performed.

You must provide the "language" property with a value matching the current conversation language.

The documentation archive contains around a thousand different documents, so in the first "list" call, it is better to provide a "path"
depending on the content that you want to find. There are folders with core information that you must read if you work with something YQL- / YDB-specific:

- "core/yql" - complete information about YQL syntax for both DDL and DML queries with examples and recipes.
    Always read this folder if the user asks you about syntax or you are unsure about specific built-in functions / DDL syntax for YDB-specific scheme entities
    (for example, if you need to work with strings / dates / URLs, etc., and process them in some way, you must read the corresponding documentation).
- "core/concepts" - top-level information about all YDB entities; this should be your entry point for discovering a YDB-specific entity before
   modifying it / answering the user about it.
- "core/dev" - folder with detailed explanations of internal YDB entities like CDC / Vector|Fulltext|Secondary indexes / Terraform / Sys view / Streaming queries.
    Read this folder to get an explanation of how you should work with YDB-specific entities before attempting to create / modify them.
- "core/recipes" - this folder contains very useful examples for advanced scenarios with YDB-specific scheme entities like transfer, vector indexes,
    streaming queries, full-text search indexes, backups (also incremental), and YDB SDK / YDB CLI examples. You must read this folder
    if you need to set up these YDB-specific features or have trouble with YDB CLI command execution.
- "core/analyst" - information for YDB analytics usage.

These folders can help you resolve failures:

- "core/troubleshooting" - read this folder when you or the user encounters some trouble with server-side query execution / high latency / spilling or shard problems, etc.
- "core/faq" - if you are not sure how to solve the user's task or have some problems that are not covered in other folders, try looking at this folder.
- "core/security" - read this folder when the user asks you about authentication settings for YDB or you encounter trouble with authorization / security access.

These are reference folders; read them if you need this information to answer the user / solve the task:

- "core/reference" - this folder contains detailed information about YDB CLI commands; read this folder if you have trouble with
    YDB CLI command execution via an exec shell tool. The folder also contains information about YDB SDK, YDB dsTOOL (managing server storage),
    Observability and UI docs, YDB cluster configuration and Docker setup, and supported YDB APIs, so also read this folder if the user asks about them.
- "core/postgresql" - read this folder if the user asks you about YDB and PostgreSQL compatibility or you want to execute queries in PG syntax (generally, you must avoid it).
- "core/maintenance" - you must read this folder if the user asks you to perform some maintenance command that will change the internal server storage state,
    such as storage disk eviction, self-healing, adding new disks, changing internal configuration, etc.
- "core/devops" - folder with complete information about DevOps tasks related to deploying and managing a YDB cluster.
- "core/integrations" - read this folder if, in the task you are solving, you need to find out about YDB capabilities in integration with
    ORM / GUI / Visualizations / Vector index search (embeddings) / Orchestration systems like Airflow / Django / Grafana / LangChain (and many others).
- "core/downloads" - folder with a description of how YDB server / YDB CLI / YDB dsTOOL can be downloaded.
- "core/contributor" - information about the implementation of some subsystems for contributors to the YDB GitHub repository.
- "core/public-materials" - commonly distributed material about YDB, such as published papers.

It is better to always choose one of these folders to list.)";

    static constexpr char ACTION_PROPERTY[] = "action";
    static constexpr char ACTION_LIST[] = "list";
    static constexpr char ACTION_GET[] = "get";
    static constexpr char ACTION_FIND[] = "find";

    static constexpr char LANG_PROPERTY[] = "language";
    static constexpr char LANG_RU[] = "ru";
    static constexpr char LANG_EN[] = "en";

    static constexpr char PATH_PROPERTY[] = "path";
    static constexpr char PATTERN_PROPERTY[] = "pattern";

    static constexpr ui64 FIND_WINDOW_SIZE = 100;
    static constexpr ui64 FIND_SIZE_LIMIT = 10_KB;

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
        if (Action != ACTION_LIST && Action != ACTION_GET && Action != ACTION_FIND) {
            throw yexception() << "Unknown action \"" << Action << "\". Expected \"" << ACTION_LIST << "\" or \"" << ACTION_GET << "\" or \"" << ACTION_FIND << "\".";
        }

        Language = to_lower(Strip(parser.GetKey(LANG_PROPERTY).GetString()));
        if (Language != LANG_RU && Language != LANG_EN) {
            throw yexception() << "Unknown language \"" << Language << "\". Expected \"" << LANG_RU << "\" or \"" << LANG_EN << "\".";
        }

        Path.clear();
        if (const auto pathParser = parser.MaybeKey(PATH_PROPERTY)) {
            Path = Strip(pathParser->GetString());

            TStringBuf pathBuf(Path);
            pathBuf.SkipPrefix("/");
            pathBuf.SkipPrefix(TStringBuf(Language));
            pathBuf.SkipPrefix("/");

            Path = pathBuf;
        }

        if (const auto patternParser = parser.MaybeKey(PATTERN_PROPERTY)) {
            Pattern = patternParser->GetString();
        }

        std::vector<TString> pagePath;
        const auto* entry = GetDocIndex().FindEntry(Path);
        if (entry) {
            pagePath = entry->Title;
        }

        if (Action == ACTION_GET) {
            if (!Path) {
                throw yexception() << "\"" << PATH_PROPERTY << "\" parameter is required for action \"" << ACTION_GET << "\" and must not be empty or equal to '/'";
            }

            if (!entry) {
                throw yexception() << "Documentation page \"" << Path << "\" was not found in the archive. Use action \"" << ACTION_LIST << "\" to discover available pages.";
            }
        } else if (!entry) {
            const auto pathBefore = Path;
            pagePath = GetDocIndex().PathToTitles(Path);
            if (pathBefore != Path) {
                YDB_CLI_LOG(Info, "Documentation listing path was changed to '" << Path << "' from '" << pathBefore << "'");
            }
        }

        TString message;
        if (Action == ACTION_LIST) {
            message = TStringBuilder() << "Listing YDB documentation pages" << (pagePath.empty() ? "" : " in folder " + JoinSeq('/', pagePath)) << "...";
        } else if (Action == ACTION_GET) {
            message = TStringBuilder() << "Reading YDB documentation page " << JoinSeq('/', pagePath) << "...";
        } else {
            if (!Pattern) {
                throw yexception() << "\"" << PATTERN_PROPERTY << "\" parameter is required for action \"" << ACTION_FIND << "\" and must not be empty";
            }
            message = TStringBuilder() << "Searching in YDB documentation" << (pagePath.empty() ? "" : " folder " + JoinSeq('/', pagePath)) << " pattern \"" << Pattern << "\"...";
        }

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
        if (Action == ACTION_FIND) {
            return DoFind();
        }
        Y_VALIDATE(false, "Unknown action: " << Action);
    }

private:
    static NJson::TJsonValue CreateParametersSchema() {
        return TJsonSchemaBuilder()
            .Property(ACTION_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Enum({ACTION_LIST, ACTION_GET, ACTION_FIND})
                .Description("Action to perform: \"list\" returns the catalog of all documentation pages, \"get\" returns the content of a single page, and \"find\" returns all occurrences of the \"pattern\" parameter via exact case-sensitive substring matching.")
                .Done()
            .Property(LANG_PROPERTY)
                .Type(TJsonSchemaBuilder::EType::String)
                .Enum({"ru", "en"})
                .Description("Documentation language to select. The same language as the current conversation language must be used. For example, if the user queries 'How can I set up my database?', you should select \"language\"=\"en\"; for the query 'Как я могу настроить мою базу данных?', you should use \"language\"=\"ru\".")
                .Done()
            .Property(PATH_PROPERTY, /* required */ false)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Archive-relative path to the documentation page (required when the action is \"get\"). Use a value returned by the \"list\" action, e.g., \"core/concepts/architecture.md\".")
                .Done()
            .Property(PATTERN_PROPERTY, /* required */ false)
                .Type(TJsonSchemaBuilder::EType::String)
                .Description("Pattern to search for in the archive; should be used with the \"find\" action. All occurrences of this pattern will be returned with a fixed context window.")
                .Done()
            .Build();
    }

    static void SaveEntryMetadata(NJson::TJsonValue& result, size_t& payloadSize, const TTocIndex::TEntry& entry) {
        result["path"] = entry.Path;
        payloadSize += entry.Path.size();

        auto& titles = result["title_path"].SetType(NJson::JSON_ARRAY).GetArraySafe();
        for (const auto& title : entry.Title) {
            titles.emplace_back(title);
            payloadSize += title.size();
        }
    }

    void EnsureLoaded() {
        if (Store) {
            return;
        }

        Y_VALIDATE(NResource::Has(DOCS_ARCHIVE_RESOURCE), "Can not load docs, no docs.archive resource: " << DOCS_ARCHIVE_RESOURCE);

        Store = TDocStore::Load();

        const auto templateResolver = std::make_shared<TTemplateResolver>(Store);
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
            if (!entry.Path.StartsWith(Path)) {
                continue;
            }

            auto& item = array.emplace_back();
            SaveEntryMetadata(item, payloadSize, entry);
        }

        YDB_CLI_LOG(Info, "Listed " << array.size() << " documentation pages in the archive, payload size: " << payloadSize);
        YDB_CLI_LOG(Debug, "Full documentation list result:\n" << FormatJsonValue(result));
        return TResponse::Success(std::move(result));
    }

    TResponse DoGet() {
        if (const auto* entry = GetDocIndex().FindEntry(Path)) {
            YDB_CLI_LOG(Info, "Extracted documentation page with size " << entry->Content.size());
            YDB_CLI_LOG(Debug, "Extracted documentation with content:\n" << Strip(entry->Content));
            return TResponse::Success(entry->Content);
        }
        return TResponse::Error(TStringBuilder() << "Documentation page \"" << Path << "\" was not found in the archive. Use action \"" << ACTION_LIST << "\" to discover available pages.");
    }

    TResponse DoFind() {
        NJson::TJsonValue result;
        ui64 payloadSize = 0;
        ui64 processedFiles = 0;
        ui64 matchesCount = 0;
        bool truncated = false;

        auto& array = result.SetType(NJson::JSON_ARRAY).GetArraySafe();
        const auto& entries = GetDocIndex().GetEntries();
        for (const auto& entry : entries) {
            if (!entry.Path.StartsWith(Path)) {
                continue;
            }

            ++processedFiles;
            std::vector<TString> matches;
            ui64 right = 0;
            for (size_t i = entry.Content.find(Pattern); i < entry.Content.size(); i = entry.Content.find(Pattern, right + 1)) {
                ++matchesCount;

                const size_t rawBegin = i - std::min(FIND_WINDOW_SIZE / 2, i);
                const size_t rawEnd = std::min(i + Pattern.size() + FIND_WINDOW_SIZE / 2 + 1, entry.Content.size());
                // Snap the window to UTF-8 char boundaries: docs contain Cyrillic, and cutting a
                // multibyte char mid-sequence yields invalid UTF-8 that the model API rejects.
                const auto [begin, end] = WidenToUtf8CharBoundaries(entry.Content, rawBegin, rawEnd);
                right = end - 1;
                matches.emplace_back(entry.Content.substr(begin, end - begin));

                if ((payloadSize += matches.back().size()) >= FIND_SIZE_LIMIT) {
                    truncated = true;
                    break;
                }
            }

            if (matches.empty()) {
                continue;
            }

            auto& item = array.emplace_back();
            SaveEntryMetadata(item, payloadSize, entry);
            item["matches"].SetType(NJson::JSON_ARRAY).GetArraySafe().assign(matches.begin(), matches.end());

            if (payloadSize >= FIND_SIZE_LIMIT) {
                truncated = true;
                break;
            }
        }

        if (truncated) {
            array.emplace_back(TStringBuilder() << "Results are truncated, processed only "
                << processedFiles << " / " << entries.size() << " files and found " << matchesCount << " matches, result size "
                << payloadSize << " exceeded allowed limit " << FIND_SIZE_LIMIT << ", try to use deepest \"path\" value or larger \"pattern\"");
        }

        YDB_CLI_LOG(Info, "Found " << matchesCount << " documentation matches for pattern \"" << Pattern << "\" in the archive, processed " << processedFiles << " files, payload size: " << payloadSize);
        YDB_CLI_LOG(Debug, "Full documentation search result:\n" << FormatJsonValue(result));
        return TResponse::Success(std::move(result));
    }

private:
    TDocStore::TPtr Store;
    TTocIndex::TPtr RuDocIndex;
    TTocIndex::TPtr EnDocIndex;
    TString Action;
    TString Language;
    TString Path;
    TString Pattern;
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
