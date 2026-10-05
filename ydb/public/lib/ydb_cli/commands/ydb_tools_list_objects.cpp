#include "ydb_tools_list_objects.h"

#include <ydb/public/lib/ydb_cli/common/normalize_path.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/hash_set.h>
#include <util/generic/is_in.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <algorithm>

namespace NYdb::NConsoleClient {

// Names of index implementation tables for one index directory.
// A failed ListDirectory throws; a successful empty directory uses the type's assumed names.
TVector<TString> IndexImplTablesFromDirectory(
    const NScheme::TListDirectoryResult& list,
    const NTable::TIndexDescription& index);

namespace {

using namespace NScheme;
using namespace NTable;

bool IsExportableSchemeObject(const TSchemeEntry& entry) {
    return IsIn({
        ESchemeEntryType::Table,
        ESchemeEntryType::ColumnTable,
        ESchemeEntryType::View,
        ESchemeEntryType::Topic,
    }, entry.Type);
}

bool IsTransientIndexImplTable(TStringBuf name) {
    return name.EndsWith("0build") || name.EndsWith("1build") || name.EndsWith("rowidsrc");
}

TString RelativeObjectName(const TString& root, const TString& fullPath) {
    const TString normalizedRoot = NormalizePath(root);
    const TString normalizedFull = NormalizePath(fullPath);
    if (normalizedFull == normalizedRoot) {
        return ".";
    }
    const TString prefix = normalizedRoot + "/";
    if (normalizedFull.StartsWith(prefix)) {
        return normalizedFull.substr(prefix.size());
    }
    return normalizedFull;
}

TVector<TString> IndexImplTableNames(const TIndexDescription& index) {
    switch (index.GetIndexType()) {
        case EIndexType::GlobalVectorKMeansTree:
            if (index.GetIndexColumns().size() <= 1) {
                return {"indexImplLevelTable", "indexImplPostingTable"};
            }
            return {"indexImplLevelTable", "indexImplPostingTable", "indexImplPrefixTable"};
        case EIndexType::GlobalFulltextRelevance:
            return {"indexImplDictTable", "indexImplDocsTable", "indexImplStatsTable", "indexImplTable"};
        case EIndexType::GlobalSync:
        case EIndexType::GlobalAsync:
        case EIndexType::GlobalUnique:
        case EIndexType::GlobalFulltextPlain:
        case EIndexType::GlobalJson:
        case EIndexType::Unknown:
        default:
            return {"indexImplTable"};
    }
}

bool IsAsyncReplicaTable(TSession& session, const TString& path) {
    const auto describeResult = session.DescribeTable(path).ExtractValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(describeResult);
    const auto& attributes = describeResult.GetTableDescription().GetAttributes();
    const auto it = attributes.find("__async_replica");
    return it != attributes.end() && it->second == "true";
}

void AppendIndexImplObjects(
    TSchemeClient& schemeClient,
    TSession& session,
    const TString& tablePath,
    const TString& tableRel,
    THashSet<TString>& names)
{
    const auto describeResult = session.DescribeTable(tablePath).ExtractValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(describeResult);

    for (const auto& index : describeResult.GetTableDescription().GetIndexDescriptions()) {
        const TString indexName = TString{index.GetIndexName()};
        const TString indexPath = Join('/', tablePath, indexName);
        const auto list = schemeClient.ListDirectory(indexPath).ExtractValueSync();
        const TVector<TString> implTables = IndexImplTablesFromDirectory(list, index);

        for (const auto& implTable : implTables) {
            if (tableRel == ".") {
                names.insert(TStringBuilder() << indexName << "/" << implTable);
            } else {
                names.insert(TStringBuilder() << tableRel << "/" << indexName << "/" << implTable);
            }
        }
    }
}

TVector<TString> ListObjects(
    TSchemeClient& schemeClient,
    TTableClient& tableClient,
    const TString& path,
    bool includeIndexData)
{
    auto listing = RecursiveList(schemeClient, path, TRecursiveListSettings().Filter(&IsExportableSchemeObject));
    NStatusHelpers::ThrowOnErrorOrPrintIssues(listing.Status);

    TVector<TSchemeEntry> entries = std::move(listing.Entries);
    NStatusHelpers::ThrowOnErrorOrPrintIssues(tableClient.RetryOperationSync([&entries](TSession session) {
        try {
            std::erase_if(entries, [&session](const TSchemeEntry& entry) {
                return entry.Type == ESchemeEntryType::Table && IsAsyncReplicaTable(session, TString{entry.Name});
            });
        } catch (NStatusHelpers::TYdbErrorException& e) {
            return e.ExtractStatus();
        }
        return TStatus(EStatus::SUCCESS, {});
    }));

    THashSet<TString> names;
    for (const auto& entry : entries) {
        const TString fullPath = TString{entry.Name};
        names.insert(RelativeObjectName(path, fullPath));
    }

    if (includeIndexData) {
        NStatusHelpers::ThrowOnErrorOrPrintIssues(tableClient.RetryOperationSync([&](TSession session) {
            try {
                for (const auto& entry : entries) {
                    if (entry.Type != ESchemeEntryType::Table) {
                        continue;
                    }
                    const TString fullPath = TString{entry.Name};
                    AppendIndexImplObjects(
                        schemeClient,
                        session,
                        fullPath,
                        RelativeObjectName(path, fullPath),
                        names);
                }
            } catch (NStatusHelpers::TYdbErrorException& e) {
                return e.ExtractStatus();
            }
            return TStatus(EStatus::SUCCESS, {});
        }));
    }

    TVector<TString> result(names.begin(), names.end());
    std::sort(result.begin(), result.end());
    return result;
}

TVector<TString> ListedIndexImplTables(const std::vector<NScheme::TSchemeEntry>& children) {
    TVector<TString> implTables;
    for (const auto& child : children) {
        const TString childName = TString{child.Name};
        if (IsTransientIndexImplTable(childName)) {
            continue;
        }
        if (child.Type == ESchemeEntryType::Table || child.Type == ESchemeEntryType::Unknown) {
            implTables.push_back(childName);
        }
    }
    return implTables;
}

} // namespace

TVector<TString> IndexImplTablesFromDirectory(
    const NScheme::TListDirectoryResult& list,
    const NTable::TIndexDescription& index)
{
    // An API error is not an empty directory. Guessing names here would write a manifest
    // that does not match the scheme.
    NStatusHelpers::ThrowOnErrorOrPrintIssues(list);
    TVector<TString> implTables = ListedIndexImplTables(list.GetChildren());
    if (implTables.empty()) {
        implTables = IndexImplTableNames(index);
    }
    return implTables;
}

TCommandListObjects::TCommandListObjects()
    : TYdbCommand("list-objects", {},
        "List schema objects under a database path, one name per line relative to --path. "
        "The output can be passed to ydb tools validate --expected-objects.")
{
}

void TCommandListObjects::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.SetFreeArgsNum(0);

    config.Opts->AddLongOption('p', "path",
            "Database path to a directory or an object. "
            "Accepts a path relative to the database or a full path that starts with the database path.")
        .DefaultValue(".").StoreResult(&Path)
        .SchemePathCompletionForAll();
    config.Opts->AddLongOption("include-index-data",
            "Also list index implementation tables that a materialized export "
            "(ydb export --include-index-data) writes as extra items.")
        .StoreTrue(&IncludeIndexData);
    config.Opts->AddLongOption('o', "output",
            "Write the list to a file. By default the list is printed to stdout.")
        .RequiredArgument("PATH").StoreResult(&OutputFile);
}

void TCommandListObjects::ExtractParams(TConfig& config) {
    TClientCommand::ExtractParams(config);
    AdjustPath(config);
}

int TCommandListObjects::Run(TConfig& config) {
    const auto driver = CreateDriver(config);
    TSchemeClient schemeClient(driver);
    TTableClient tableClient(driver);

    const TVector<TString> names = ListObjects(schemeClient, tableClient, Path, IncludeIndexData);

    if (OutputFile) {
        TFileOutput output(OutputFile);
        for (const auto& name : names) {
            output << name << Endl;
        }
    } else {
        for (const auto& name : names) {
            Cout << name << Endl;
        }
    }

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
