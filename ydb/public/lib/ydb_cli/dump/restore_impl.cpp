#include "restore_impl.h"
#include "restore_import_data.h"
#include "restore_compat.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <format>
#include <re2/re2.h>

namespace NYdb {
namespace NDump {

using namespace NImport;
using namespace NOperation;
using namespace NScheme;
using namespace NTable;

extern const char DOC_API_TABLE_VERSION_ATTR[] = "__document_api_version";
extern const char DOC_API_REQUEST_TYPE[] = "_document_api_request";

namespace {

bool IsFileExists(const TFsPath& path) {
    return path.Exists() && path.IsFile();
}

Ydb::Table::CreateTableRequest ReadTableScheme(const TString& fsPath) {
    Ydb::Table::CreateTableRequest proto;
    Y_ENSURE(google::protobuf::TextFormat::ParseFromString(TFileInput(fsPath).ReadAll(), &proto));
    return proto;
}

TTableDescription TableDescriptionFromProto(const Ydb::Table::CreateTableRequest& proto) {
    return TProtoAccessor::FromProto(proto);
}

bool HasRunningIndexBuilds(TOperationClient& client, const TString& dbPath) {
    const ui64 pageSize = 100;
    TString pageToken;

    do {
        const ui32 maxRetries = 8;
        TDuration retrySleep = TDuration::MilliSeconds(100);

        for (ui32 retryNumber = 0; retryNumber <= maxRetries; ++retryNumber) {
            auto operations = client.List<TBuildIndexOperation>(pageSize, pageToken).GetValueSync();

            if (!operations.IsSuccess()) {
                if (retryNumber == maxRetries) {
                    Y_ENSURE(false, "Cannot list operations");
                }

                switch (operations.GetStatus()) {
                case EStatus::ABORTED:
                    break;
                case EStatus::OVERLOADED:
                case EStatus::CLIENT_RESOURCE_EXHAUSTED:
                case EStatus::UNAVAILABLE:
                case EStatus::TRANSPORT_UNAVAILABLE:
                    NConsoleClient::ExponentialBackoff(retrySleep);
                    break;
                default:
                    Y_ENSURE(false, "Unexpected status while trying to list operations: " << operations.GetStatus());
                }

                continue;
            }

            for (const auto& operation : operations.GetList()) {
                if (operation.Metadata().Path != dbPath) {
                    continue;
                }

                switch (operation.Metadata().State) {
                case EBuildIndexState::Preparing:
                case EBuildIndexState::TransferData:
                case EBuildIndexState::Applying:
                case EBuildIndexState::Cancellation:
                case EBuildIndexState::Rejection:
                    return true;
                default:
                    break;
                }
            }

            pageToken = operations.NextPageToken();
        }
    } while (pageToken != "0");

    return false;
}

TString GetBackupRoot(TStringInput query) {
    TString backupRoot;

    constexpr TStringBuf targetLinePrefix = "-- backup root: \"";
    constexpr TStringBuf discardedSuffix = "\"";
    TString line;
    while (query.ReadLine(line)) {
        if (line.StartsWith(targetLinePrefix)) {
            backupRoot = line.substr(
                std::size(targetLinePrefix),
                std::size(line) - std::size(targetLinePrefix) - std::size(discardedSuffix)
            );
            return backupRoot;
        }
    }

    return backupRoot;
}

bool IsDatabase(TSchemeClient& client, const TString& path) {
    auto result = DescribePath(client, path);
    return result.GetStatus() == EStatus::SUCCESS && result.GetEntry().Type == ESchemeEntryType::SubDomain;
}

bool RewriteTablePathPrefix(TString& query, TStringBuf backupRoot, TStringBuf restoreRoot,
    bool restoreRootIsDatabase, NYql::TIssues& issues
) {
    if (backupRoot == restoreRoot) {
        return true;
    }

    TString pathPrefix;
    if (!re2::RE2::PartialMatch(query, R"(PRAGMA TablePathPrefix = '(\S+)';)", &pathPrefix)) {
        if (!restoreRootIsDatabase) {
            // Initially, the view used the implicit table path prefix, but this is no longer feasible
            // since the restore root is different from the database root.
            // Consequently, we must issue an explicit TablePathPrefix pragma to ensure that the reference targets
            // maintain the same relative positions to the view's location as they did previously.

            size_t contextRecreationEnd = query.find("CREATE VIEW");
            if (contextRecreationEnd == TString::npos) {
                issues.AddIssue(TStringBuilder() << "no create view statement in the query: " << query);
                return false;
            }
            query.insert(contextRecreationEnd, TString(
                std::format("PRAGMA TablePathPrefix = '{}';\n", restoreRoot.data())
            ));
        }
        return true;
    }

    pathPrefix = RewriteAbsolutePath(pathPrefix, backupRoot, restoreRoot);

    constexpr TStringBuf pattern = R"(PRAGMA TablePathPrefix = '\S+';)";
    if (!re2::RE2::Replace(&query, pattern,
        std::format(R"(PRAGMA TablePathPrefix = '{}';)", pathPrefix.c_str())
    )) {
        issues.AddIssue(TStringBuilder() << "query: " << query.Quote()
            << " does not contain the pattern: \"" << pattern << "\""
        );
        return false;
    }

    return true;
}

} // anonymous

TRestoreClient::TRestoreClient(
        TImportClient& importClient,
        TOperationClient& operationClient,
        TSchemeClient& schemeClient,
        TTableClient& tableClient,
        NQuery::TQueryClient& queryClient)
    : ImportClient(importClient)
    , OperationClient(operationClient)
    , SchemeClient(schemeClient)
    , TableClient(tableClient)
    , QueryClient(queryClient)
{
}

TRestoreResult TRestoreClient::Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings) {
    // find existing items
    TFsPath dbBasePath = dbPath;

    while (true) {
        auto result = DescribePath(SchemeClient, dbBasePath).GetStatus();

        if (result == EStatus::SUCCESS) {
            break;
        }

        if (result != EStatus::SCHEME_ERROR) {
            return Result<TRestoreResult>(EStatus::SCHEME_ERROR, "Can not find existing path");
        }

        dbBasePath = dbBasePath.Parent();
    }

    auto oldDirectoryList = SchemeClient.ListDirectory(dbBasePath).GetValueSync();
    if (!oldDirectoryList.IsSuccess()) {
        return Result<TRestoreResult>(EStatus::SCHEME_ERROR, "Can not list existing directory");
    }

    THashSet<TString> oldEntries;
    for (const auto& entry : oldDirectoryList.GetChildren()) {
        oldEntries.insert(entry.Name);
    }

    // restore
    auto restoreResult = RestoreFolder(fsPath, dbPath, "", settings);

    if (!ViewRestorationCalls.empty()) {
        TVector<TRestoreViewCall> calls;
        TMaybe<TRestoreResult> lastFail;
        size_t size;
        do {
            calls.clear();
            lastFail.Clear();
            size = ViewRestorationCalls.size();
            std::swap(calls, ViewRestorationCalls);

            for (auto& [fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings, isAlreadyExisting] : calls) {
                auto result = RestoreView(fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings);
                if (!result.IsSuccess()) {
                    lastFail = std::move(result);
                }
            }
        } while (!ViewRestorationCalls.empty() && ViewRestorationCalls.size() < size);

        // retries could not fix the errors
        if (!ViewRestorationCalls.empty() || lastFail) {
            restoreResult = *lastFail;
        }
    }

    if (restoreResult.IsSuccess() || settings.SavePartialResult_) {
        return restoreResult;
    }

    // cleanup
    auto newDirectoryList = SchemeClient.ListDirectory(dbBasePath).GetValueSync();
    if (!newDirectoryList.IsSuccess()) {
        return restoreResult;
    }

    for (const auto& entry : newDirectoryList.GetChildren()) {
        if (oldEntries.contains(entry.Name)) {
            continue;
        }

        auto fullPath = dbBasePath.Child(entry.Name);

        switch (entry.Type) {
            case ESchemeEntryType::Directory: {
                auto result = RemoveDirectoryRecursive(SchemeClient, TableClient, nullptr, &QueryClient, fullPath, NConsoleClient::ERecursiveRemovePrompt::Never, {}, true, false);
                if (!result.IsSuccess()) {
                    return restoreResult;
                }
                break;
            }
            case ESchemeEntryType::Table: {
                auto result = TableClient.RetryOperationSync([path = fullPath](TSession session) {
                    return session.DropTable(path).GetValueSync();
                });
                if (!result.IsSuccess()) {
                    return restoreResult;
                }
                break;
            }
            case ESchemeEntryType::View: {
                auto result = NConsoleClient::RetryFunction([&client = QueryClient, &fullPath]() {
                    return client.ExecuteQuery(std::format(R"(
                        DROP VIEW IF EXISTS `{}`;
                    )", fullPath.c_str()), NQuery::TTxControl::NoTx()).ExtractValueSync();
                });
                if (!result.IsSuccess()) {
                    return restoreResult;
                }
                break;
            }
            default:
                return restoreResult;
        }
    }

    return restoreResult;
}

TRestoreResult TRestoreClient::RestoreFolder(const TFsPath& fsPath, const TString& dbRestoreRoot, const TString& dbPathRelativeToRestoreRoot, const TRestoreSettings& settings) {
    const TString dbPath = dbRestoreRoot + dbPathRelativeToRestoreRoot;

    if (!fsPath) {
        return Result<TRestoreResult>(EStatus::BAD_REQUEST, "Folder is not specified");
    }

    if (!fsPath.Exists()) {
        return Result<TRestoreResult>(EStatus::BAD_REQUEST,
            TStringBuilder() << "Specified folder does not exist: " << fsPath.GetPath());
    }

    if (!fsPath.IsDirectory()) {
        return Result<TRestoreResult>(EStatus::BAD_REQUEST,
            TStringBuilder() << "Specified folder is not a directory: " << fsPath.GetPath());
    }

    if (IsFileExists(fsPath.Child(INCOMPLETE_FILE_NAME))) {
        return Result<TRestoreResult>(EStatus::BAD_REQUEST,
            TStringBuilder() << "There is incomplete file in folder: " << fsPath.GetPath());
    }

    const TString objectDbPath = Join('/', dbPath, fsPath.GetName());

    if (IsFileExists(fsPath.Child(SCHEME_FILE_NAME))) {
        return RestoreTable(fsPath, objectDbPath, settings);
    }

    if (IsFileExists(fsPath.Child(CREATE_VIEW_FILE_NAME))) {
        // delay view restoration
        ViewRestorationCalls.emplace_back(fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot);
        return Result<TRestoreResult>();
    }

    if (IsFileExists(fsPath.Child(EMPTY_FILE_NAME))) {
        return MakeDirectory(SchemeClient, Join('/', dbPath, fsPath.GetName()));
    }

    TMaybe<TRestoreResult> result;

    TVector<TFsPath> children;
    fsPath.List(children);
    for (const auto& child : children) {
        const TString childDbPath = Join('/', dbPath, child.GetName());
        if (IsFileExists(child.Child(SCHEME_FILE_NAME))) {
            result = RestoreTable(child, childDbPath, settings);
        } else if (IsFileExists(child.Child(EMPTY_FILE_NAME))) {
            result = MakeDirectory(SchemeClient, childDbPath);
        } else if (IsFileExists(child.Child(CREATE_VIEW_FILE_NAME))) {
            // delay view restoration
            ViewRestorationCalls.emplace_back(child, dbRestoreRoot, Join('/', dbPathRelativeToRestoreRoot, child.GetName()), settings);
        } else {
            result = RestoreFolder(child, dbRestoreRoot, Join('/', dbPathRelativeToRestoreRoot, child.GetName()), settings);
        }

        if (result && !result->IsSuccess()) {
            return *result;
        }
    }

    if (!result) {
        return Result<TRestoreResult>();
    }

    return *result;
}

TRestoreResult TRestoreClient::RestoreView(
    const TFsPath& fsPath,
    const TString& dbRestoreRoot,
    const TString& dbPathRelativeToRestoreRoot,
    const TRestoreSettings& settings
) {
    if (fsPath.Child(INCOMPLETE_FILE_NAME).Exists()) {
        return Result<TRestoreResult>(EStatus::BAD_REQUEST,
            TStringBuilder() << "There is incomplete file in folder: " << fsPath.GetPath().Quote()
        );
    }

    const TString dbPath = dbRestoreRoot + dbPathRelativeToRestoreRoot;

    const auto createViewFile = fsPath.Child(CREATE_VIEW_FILE_NAME);
    TString query = TFileInput(createViewFile).ReadAll();

    const auto backupRoot = GetBackupRoot(query);
    {
        NYql::TIssues issues;
        if (!RewriteTablePathPrefix(query, backupRoot, dbRestoreRoot, IsDatabase(SchemeClient, dbRestoreRoot), issues)) {
            // hard fail since we want to avoid silent fails with wrong table path prefixes
            return Result<TRestoreResult>(dbPath, TStatus(EStatus::BAD_REQUEST, std::move(issues)));
        }
    }
    {
        NYql::TIssues issues;
        RewriteTableRefs(query, backupRoot, dbRestoreRoot, issues);
    }

    constexpr TStringBuf pattern = R"(CREATE VIEW IF NOT EXISTS `\S+` )";
    if (!re2::RE2::Replace(&query, pattern, std::format(R"(CREATE VIEW IF NOT EXISTS `{}` )", dbPath.c_str()))) {
        NYql::TIssues issues;
        issues.AddIssue(TStringBuilder() << "Cannot restore a view from the file: " << createViewFile.GetPath().Quote()
            << ". Pattern: \"" << pattern << "\", was not found in the create view statement: " << query.Quote()
        );
        return Result<TRestoreResult>(dbPath, TStatus(EStatus::BAD_REQUEST, std::move(issues)));
    }

    if (settings.DryRun_) {
        auto pathDescription = DescribePath(SchemeClient, dbPath);
        if (!pathDescription.IsSuccess()) {
            return Result<TRestoreResult>(dbPath, std::move(pathDescription));
        }
        if (pathDescription.GetEntry().Type != NScheme::ESchemeEntryType::View) {
            return Result<TRestoreResult>(dbPath, EStatus::SCHEME_ERROR,
                TStringBuilder() << "expected a view, got: " << pathDescription.GetEntry().Type
            );
        }

        return Result<TRestoreResult>();
    }

    auto creationResult = NConsoleClient::RetryFunction([&client = QueryClient, &query]() {
        return client.ExecuteQuery(
            query,
            NQuery::TTxControl::NoTx()
        ).ExtractValueSync();
    });

    if (creationResult.IsSuccess()) {
        return Result<TRestoreResult>();
    }

    if (creationResult.GetStatus() == EStatus::SCHEME_ERROR) {
        // Scheme error happens when the view depends on a table (or a view) that is not yet restored.
        // Instead of tracking view dependencies, we simply retry the creation of the view later.
        ViewRestorationCalls.emplace_back(fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings);
    }
    return Result<TRestoreResult>(dbPath, std::move(creationResult));
}

TRestoreResult TRestoreClient::RestoreTable(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings) {
    if (fsPath.Child(INCOMPLETE_FILE_NAME).Exists()) {
        return Result<TRestoreResult>(EStatus::BAD_REQUEST,
            TStringBuilder() << "There is incomplete file in folder: " << fsPath.GetPath());
    }

    auto scheme = ReadTableScheme(fsPath.Child(SCHEME_FILE_NAME));
    auto dumpedDesc = TableDescriptionFromProto(scheme);

    if (settings.DryRun_) {
        return CheckSchema(dbPath, dumpedDesc);
    }

    if (settings.RestoreData_) {
        auto woIndexes = scheme;
        woIndexes.clear_indexes();

        auto result = RestoreData(fsPath, dbPath, settings, TableDescriptionFromProto(woIndexes));
        if (!result.IsSuccess()) {
            return result;
        }
    }

    if (settings.RestoreIndexes_) {
        auto result = RestoreIndexes(dbPath, dumpedDesc);
        if (!result.IsSuccess()) {
            return result;
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::CheckSchema(const TString& dbPath, const TTableDescription& desc) {
    TMaybe<TTableDescription> actualDesc;
    auto descResult = DescribeTable(TableClient, dbPath, actualDesc);
    if (!descResult.IsSuccess()) {
        return Result<TRestoreResult>(dbPath, std::move(descResult));
    }

    auto unorderedColumns = [](const TVector<TColumn>& orderedColumns) {
        THashMap<TString, TColumn> result;
        for (const auto& column : orderedColumns) {
            result.emplace(column.Name, column);
        }
        return result;
    };

    auto unorderedIndexes = [](const TVector<TIndexDescription>& orderedIndexes) {
        THashMap<TString, TIndexDescription> result;
        for (const auto& index : orderedIndexes) {
            result.emplace(index.GetIndexName(), index);
        }
        return result;
    };

    if (unorderedColumns(desc.GetColumns()) != unorderedColumns(actualDesc->GetColumns())) {
        return Result<TRestoreResult>(dbPath, EStatus::SCHEME_ERROR, TStringBuilder() << "Columns differ"
            << ": dumped# " << JoinSeq(", ", desc.GetColumns())
            << ", actual# " << JoinSeq(", ", actualDesc->GetColumns()));
    }

    if (desc.GetPrimaryKeyColumns() != actualDesc->GetPrimaryKeyColumns()) {
        return Result<TRestoreResult>(dbPath, EStatus::SCHEME_ERROR, TStringBuilder() << "Primary key columns differ"
            << ": dumped# " << JoinSeq(", ", desc.GetPrimaryKeyColumns())
            << ", actual# " << JoinSeq(", ", actualDesc->GetPrimaryKeyColumns()));
    }

    if (unorderedIndexes(desc.GetIndexDescriptions()) != unorderedIndexes(actualDesc->GetIndexDescriptions())) {
        return Result<TRestoreResult>(dbPath, EStatus::SCHEME_ERROR, TStringBuilder() << "Indexes differ"
            << ": dumped# " << JoinSeq(", ", desc.GetIndexDescriptions())
            << ", actual# " << JoinSeq(", ", actualDesc->GetIndexDescriptions()));
    }

    return Result<TRestoreResult>();
}

struct TWriterWaiter {
    NPrivate::IDataWriter& Writer;

    TWriterWaiter(NPrivate::IDataWriter& writer)
        : Writer(writer)
    {
    }

    ~TWriterWaiter() {
        Writer.Wait();
    }
};

TRestoreResult TRestoreClient::RestoreData(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const TTableDescription& desc) {
    if (desc.GetAttributes().contains(DOC_API_TABLE_VERSION_ATTR) && settings.SkipDocumentTables_) {
        return Result<TRestoreResult>();
    }

    auto createResult = TableClient.RetryOperationSync([&dbPath, &desc](TSession session) {
        return session.CreateTable(dbPath, TTableDescription(desc),
            TCreateTableSettings().RequestType(DOC_API_REQUEST_TYPE)).GetValueSync();
    });
    if (!createResult.IsSuccess()) {
        return Result<TRestoreResult>(dbPath, std::move(createResult));
    }

    THolder<NPrivate::IDataAccumulator> accumulator;
    THolder<NPrivate::IDataWriter> writer;

    switch (settings.Mode_) {
        case TRestoreSettings::EMode::Yql:
        case TRestoreSettings::EMode::BulkUpsert: {
            accumulator.Reset(CreateCompatAccumulator(dbPath, desc, settings));
            writer.Reset(CreateCompatWriter(dbPath, TableClient, accumulator.Get(), settings));

            break;
        }

        case TRestoreSettings::EMode::ImportData: {
            TMaybe<TTableDescription> actualDesc;
            auto descResult = DescribeTable(TableClient, dbPath, actualDesc);
            if (!descResult.IsSuccess()) {
                return Result<TRestoreResult>(dbPath, std::move(descResult));
            }

            accumulator.Reset(CreateImportDataAccumulator(desc, *actualDesc, settings));
            writer.Reset(CreateImportDataWriter(dbPath, desc, ImportClient, TableClient, accumulator.Get(), settings));

            break;
        }
    }

    TWriterWaiter waiter(*writer);
    ui32 dataFileId = 0;
    TFsPath dataFile = fsPath.Child(DataFileName(dataFileId));

    while (dataFile.Exists()) {
        TFileInput input(dataFile, settings.FileBufferSize_);
        TString line;

        while (input.ReadLine(line)) {
            while (!accumulator->Fits(line)) {
                if (!accumulator->Ready(true)) {
                    return Result<TRestoreResult>(dbPath, EStatus::INTERNAL_ERROR, "Data is not ready");
                }

                if (!writer->Push(accumulator->GetData(true))) {
                    return Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #1");
                }
            }

            accumulator->Feed(std::move(line));
            if (accumulator->Ready()) {
                if (!writer->Push(accumulator->GetData())) {
                    return Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #2");
                }
            }
        }

        dataFile = fsPath.Child(DataFileName(++dataFileId));
    }

    while (accumulator->Ready(true)) {
        if (!writer->Push(accumulator->GetData(true))) {
            return Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #3");
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreIndexes(const TString& dbPath, const TTableDescription& desc) {
    TMaybe<TTableDescription> actualDesc;

    for (const auto& index : desc.GetIndexDescriptions()) {
        // check (and wait) for unuexpected index buils
        while (HasRunningIndexBuilds(OperationClient, dbPath)) {
            actualDesc.Clear();
            Sleep(TDuration::Minutes(1));
        }

        if (!actualDesc) {
            auto descResult = DescribeTable(TableClient, dbPath, actualDesc);
            if (!descResult.IsSuccess()) {
                return Result<TRestoreResult>(dbPath, std::move(descResult));
            }
        }

        if (FindPtr(actualDesc->GetIndexDescriptions(), index)) {
            continue;
        }

        auto status = TableClient.RetryOperationSync([&dbPath, &index](TSession session) {
            auto settings = TAlterTableSettings().AppendAddIndexes(index);
            return session.AlterTableLong(dbPath, settings).GetValueSync().Status();
        });
        if (!status.IsSuccess() && status.GetStatus() != EStatus::STATUS_UNDEFINED) {
            return Result<TRestoreResult>(dbPath, std::move(status));
        }

        // wait for expected index build
        while (HasRunningIndexBuilds(OperationClient, dbPath)) {
            Sleep(TDuration::Minutes(1));
        }
    }

    return Result<TRestoreResult>();
}

} // NDump
} // NYdb
