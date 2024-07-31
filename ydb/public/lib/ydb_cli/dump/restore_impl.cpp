#include "restore_impl.h"
#include "restore_import_data.h"
#include "restore_compat.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/join.h>

namespace NYdb {
namespace NDump {

using namespace NConsoleClient;
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

Ydb::Scheme::ModifyPermissionsRequest ReadPermissions(const TString& fsPath) {
    Ydb::Scheme::ModifyPermissionsRequest proto;
    Y_ENSURE(google::protobuf::TextFormat::ParseFromString(TFileInput(fsPath).ReadAll(), &proto));
    return proto;
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

} // anonymous

TRestoreClient::TRestoreClient(
        TImportClient& importClient,
        TOperationClient& operationClient,
        TSchemeClient& schemeClient,
        TTableClient& tableClient)
    : ImportClient(importClient)
    , OperationClient(operationClient)
    , SchemeClient(schemeClient)
    , TableClient(tableClient)
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

   
    auto oldDirectoryList = RecursiveList(SchemeClient, dbBasePath);
    if (!oldDirectoryList.Status.IsSuccess()) {
        return Result<TRestoreResult>(EStatus::SCHEME_ERROR, "Can not list existing directory");
    }

    THashSet<TString> oldEntries;
    for (const auto& entry : oldDirectoryList.Entries) {
        oldEntries.insert(entry.Name);
    }

    // restore
    auto restoreResult = RestoreFolder(fsPath, dbPath, settings, oldEntries);
    if (restoreResult.IsSuccess() || settings.SavePartialResult_) {
        return restoreResult;
    }

    // cleanup
    auto newDirectoryList = RecursiveList(SchemeClient, dbBasePath);
    if (!newDirectoryList.Status.IsSuccess()) {
        return restoreResult;
    }

    for (const auto& entry : newDirectoryList.Entries) {
        if (oldEntries.contains(entry.Name)) {
            continue;
        }

        switch (entry.Type) {
            case ESchemeEntryType::Directory: {
                auto result = NConsoleClient::RemoveDirectoryRecursive(SchemeClient, TableClient, entry.Name, {}, true, false);
                if (!result.IsSuccess()) {
                    return restoreResult;
                }
                break;
            }
            case ESchemeEntryType::Table: {
                auto result = TableClient.RetryOperationSync([path = entry.Name](TSession session) {
                    return session.DropTable(path).GetValueSync();
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

TRestoreResult TRestoreClient::RestoreFolder(const TFsPath& fsPath, const TString& dbPath,
    const TRestoreSettings& settings, const THashSet<TString>& oldEntries)
{
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

    if (IsFileExists(fsPath.Child(SCHEME_FILE_NAME))) {
        return RestoreTable(fsPath, Join('/', dbPath, fsPath.GetName()), settings, oldEntries);
    }

    if (IsFileExists(fsPath.Child(EMPTY_FILE_NAME))) {
        auto result = MakeDirectory(SchemeClient, Join('/', dbPath, fsPath.GetName()));
        if (!result.IsSuccess()) {
            return result;
        }
        return RestorePermissions(fsPath, Join('/', dbPath, fsPath.GetName()), settings, oldEntries);
    }

    TMaybe<TRestoreResult> result;

    TVector<TFsPath> children;
    fsPath.List(children);
    for (const auto& child : children) {
        if (IsFileExists(child.Child(SCHEME_FILE_NAME))) {
            result = RestoreTable(child, Join('/', dbPath, child.GetName()), settings, oldEntries);
        } else if (IsFileExists(child.Child(EMPTY_FILE_NAME))) {
            result = MakeDirectory(SchemeClient, Join('/', dbPath, child.GetName()));
            if (result.Defined() && !result->IsSuccess()) {
                return *result;
            }
            result = RestorePermissions(fsPath, Join('/', dbPath, child.GetName()), settings, oldEntries);
        } else if (child.IsDirectory()) {
            result = RestoreFolder(child, Join('/', dbPath, child.GetName()), settings, oldEntries);
        }

        if (result.Defined() && !result->IsSuccess()) {
            return *result;
        }
    }
   
    return RestorePermissions(fsPath, dbPath, settings, oldEntries);
}

TRestoreResult TRestoreClient::RestoreTable(const TFsPath& fsPath, const TString& dbPath,
    const TRestoreSettings& settings, const THashSet<TString>& oldEntries)
{
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

    return RestorePermissions(fsPath, dbPath, settings, oldEntries);
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

TRestoreResult TRestoreClient::RestorePermissions(const TFsPath& fsPath, const TString& dbPath,
    const TRestoreSettings& settings, const THashSet<TString>& oldEntries)
{   
    if (oldEntries.contains(dbPath)) {
        return Result<TRestoreResult>();
    }

    if (!fsPath.Child(PERMISSIONS_FILE_NAME).Exists()) {
        return Result<TRestoreResult>();
    }

    auto permissions = ReadPermissions(fsPath.Child(PERMISSIONS_FILE_NAME));
    return ModifyPermissions(SchemeClient, dbPath, TModifyPermissionsSettings(permissions));
}

} // NDump
} // NYdb
