#include "restore_impl.h"
#include "restore_import_data.h"
#include "restore_compat.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/lib/ydb_cli/dump/util/log.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/join.h>

#include <google/protobuf/text_format.h>

#include <format>

namespace NYdb::NDump {

using namespace NConsoleClient;
using namespace NImport;
using namespace NOperation;
using namespace NScheme;
using namespace NTable;
using namespace NTopic;

extern const char DOC_API_TABLE_VERSION_ATTR[] = "__document_api_version";
extern const char DOC_API_REQUEST_TYPE[] = "_document_api_request";

namespace {

bool IsFileExists(const TFsPath& path) {
    return path.Exists() && path.IsFile();
}

template <typename TProtoType>
TProtoType ReadProtoFromFile(const TFsPath& fsDirPath, const TLog* log, const NFiles::TFileInfo& fileInfo) {
    const auto fsPath = fsDirPath.Child(fileInfo.FileName);
    LOG_IMPL(log, ELogPriority::TLOG_DEBUG, "Read " << fileInfo.LogObjectType << " from " << fsPath.GetPath().Quote());
    TProtoType proto;
    Y_ENSURE(google::protobuf::TextFormat::ParseFromString(TFileInput(fsPath).ReadAll(), &proto));
    return proto;
}

Ydb::Table::CreateTableRequest ReadTableScheme(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::Table::CreateTableRequest>(fsDirPath, log, NFiles::TableScheme());
}

Ydb::Table::ChangefeedDescription ReadChangefeedDescription(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::Table::ChangefeedDescription>(fsDirPath, log, NFiles::Changefeed());
}

Ydb::Topic::DescribeTopicResult ReadTopicDescription(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::Topic::DescribeTopicResult>(fsDirPath, log, NFiles::TopicDescription());
}

Ydb::Topic::CreateTopicRequest ReadTopicCreationRequest(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::Topic::CreateTopicRequest>(fsDirPath, log, NFiles::CreateTopic());
}

Ydb::Scheme::ModifyPermissionsRequest ReadPermissions(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::Scheme::ModifyPermissionsRequest>(fsDirPath, log, NFiles::Permissions());
}

TTableDescription TableDescriptionFromProto(const Ydb::Table::CreateTableRequest& proto) {
    return TProtoAccessor::FromProto(proto);
}

TChangefeedDescription ChangefeedDescriptionFromProto(const Ydb::Table::ChangefeedDescription& proto) {
    return TProtoAccessor::FromProto(proto);
}

TTopicDescription TopicDescriptionFromProto(Ydb::Topic::DescribeTopicResult&& proto) {
    return TTopicDescription(std::move(proto));
}

TTableDescription TableDescriptionWithoutIndexesFromProto(Ydb::Table::CreateTableRequest proto) {
    proto.clear_indexes();
    return TableDescriptionFromProto(proto);
}

TStatus WaitForIndexBuild(TOperationClient& client, const TOperation::TOperationId& id) {
    TDuration retrySleep = TDuration::MilliSeconds(100);
    while (true) {
        auto operation = client.Get<TBuildIndexOperation>(id).GetValueSync();
        if (!operation.Status().IsTransportError()) {
            switch (operation.Status().GetStatus()) {
                case EStatus::OVERLOADED:
                case EStatus::UNAVAILABLE:
                case EStatus::STATUS_UNDEFINED:
                    break; // retry
                default:
                    return operation.Status();
            }
        }
        NConsoleClient::ExponentialBackoff(retrySleep, TDuration::Minutes(1));
    }
}

bool IsOperationStarted(TStatus operationStatus) {
    return operationStatus.IsSuccess() || operationStatus.GetStatus() == EStatus::STATUS_UNDEFINED;
}

TVector<TFsPath> CollectDataFiles(const TFsPath& fsPath) {
    TVector<TFsPath> dataFiles;
    ui32 dataFileId = 0;
    TFsPath dataFile = fsPath.Child(DataFileName(dataFileId));
    while (dataFile.Exists()) {
        dataFiles.push_back(std::move(dataFile));
        dataFile = fsPath.Child(DataFileName(++dataFileId));
    }
    return dataFiles;
}

TRestoreResult CombineResults(const TVector<TRestoreResult>& results) {
    for (auto result : results) {
        if (!result.IsSuccess()) {
            return result;
        }
    }
    return Result<TRestoreResult>();
}

bool IsDatabase(TSchemeClient& client, const TString& path) {
    auto result = DescribePath(client, path);
    return result.GetStatus() == EStatus::SUCCESS && result.GetEntry().Type == ESchemeEntryType::SubDomain;
}

TMaybe<TRestoreResult> ErrorOnIncomplete(const TFsPath& fsPath) {
    if (fsPath.Child(NFiles::Incomplete().FileName).Exists()) {
        return Result<TRestoreResult>(EStatus::BAD_REQUEST,
            TStringBuilder() << "There is incomplete file in folder: " << fsPath.GetPath().Quote()
        );
    }
    return Nothing();
}

TRestoreResult CheckExistenceAndType(TSchemeClient& client, const TString& dbPath, ESchemeEntryType expectedType) {
    auto pathDescription = DescribePath(client, dbPath);
    if (!pathDescription.IsSuccess()) {
        return Result<TRestoreResult>(dbPath, std::move(pathDescription));
    }
    if (pathDescription.GetEntry().Type != expectedType) {
        return Result<TRestoreResult>(dbPath, EStatus::SCHEME_ERROR,
            TStringBuilder() << "Expected a " << expectedType << ", but got: " << pathDescription.GetEntry().Type
        );
    }

    return Result<TRestoreResult>();
}

TStatus CreateTopic(TTopicClient& client, const TString& dbPath, const Ydb::Topic::CreateTopicRequest& request) {
    const auto settings = TCreateTopicSettings(request);
    auto result = NConsoleClient::RetryFunction([&]() {
        return client.CreateTopic(dbPath, settings).ExtractValueSync();
    });
    return result;
}

} // anonymous

namespace NPrivate {

TLocation::TLocation(TStringBuf file, ui64 lineNo)
    : File(file)
    , LineNo(lineNo)
{
}

void TLocation::Out(IOutputStream& out) const {
    out << File << ":" << LineNo;
}

TLine::TLine(TString&& data, TStringBuf file, ui64 lineNo)
    : Data(std::move(data))
    , Location(file, lineNo)
{
}

TLine::TLine(TString&& data, const TLocation& location)
    : Data(std::move(data))
    , Location(location)
{
}

void TBatch::Add(const TLine& line) {
    Data << line.GetData() << "\n";
    Locations.push_back(line.GetLocation());
}

TString TBatch::GetLocation() const {
    THashMap<TStringBuf, std::pair<ui64, ui64>> locations;
    for (const auto& location : Locations) {
        auto it = locations.find(location.File);
        if (it == locations.end()) {
            it = locations.emplace(location.File, std::make_pair(Max<ui64>(), Min<ui64>())).first;
        }
        it->second.first = Min(location.LineNo, it->second.first);
        it->second.second = Max(location.LineNo, it->second.second);
    }

    TStringBuilder result;
    bool comma = false;
    for (const auto& [file, range] : locations) {
        if (comma) {
            result << ", ";
        }
        result << file << ":" << range.first << "-" << range.second;
        comma = true;
    }

    return result;
}

} // NPrivate

TRestoreClient::TRestoreClient(const TDriver& driver, const std::shared_ptr<TLog>& log)
    : ImportClient(driver)
    , OperationClient(driver)
    , SchemeClient(driver)
    , TableClient(driver)
    , TopicClient(driver)
    , QueryClient(driver)
    , Log(log)
{
}

TRestoreResult TRestoreClient::Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings) {
    LOG_I("Restore " << fsPath.Quote() << " to " << dbPath.Quote());

    // find existing items
    TFsPath dbBasePath = dbPath;

    while (true) {
        auto result = DescribePath(SchemeClient, dbBasePath);

        if (result.GetStatus() == EStatus::SUCCESS) {
            break;
        }

        if (result.GetStatus() != EStatus::SCHEME_ERROR) {
            LOG_E("Error finding db base path: " << result.GetIssues().ToOneLineString());
            return Result<TRestoreResult>(EStatus::SCHEME_ERROR, "Can not find existing path");
        }

        dbBasePath = dbBasePath.Parent();
    }

    LOG_D("Resolved db base path: " << dbBasePath.GetPath().Quote());

    auto oldDirectoryList = RecursiveList(SchemeClient, dbBasePath);
    if (!oldDirectoryList.Status.IsSuccess()) {
        LOG_E("Error listing db base path: " << dbBasePath.GetPath().Quote() << ": " << oldDirectoryList.Status.GetIssues().ToOneLineString());
        return Result<TRestoreResult>(EStatus::SCHEME_ERROR, "Can not list existing directory");
    }

    THashSet<TString> oldEntries;
    for (const auto& entry : oldDirectoryList.Entries) {
        oldEntries.insert(TString{entry.Name});
    }

    // restore
    auto restoreResult = RestoreFolder(fsPath, dbPath, "", settings, oldEntries);

    if (!ViewRestorationCalls.empty()) {
        TVector<TRestoreViewCall> calls;
        TMaybe<TRestoreResult> lastFail;
        size_t size;
        do {
            calls.clear();
            lastFail.Clear();
            size = ViewRestorationCalls.size();
            std::swap(calls, ViewRestorationCalls);

            for (const auto& [fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings, isAlreadyExisting] : calls) {
                auto result = RestoreView(fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings, isAlreadyExisting);
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

    if (restoreResult.IsSuccess()) {
        LOG_I("Restore completed successfully");
        return restoreResult;
    }

    LOG_E("Restore failed: " << restoreResult.GetIssues().ToOneLineString());
    if (settings.SavePartialResult_) {
        LOG_I("Partial result saved");
        return restoreResult;
    }

    LOG_I("Cleanup");

    // cleanup
    auto newDirectoryList = RecursiveList(SchemeClient, dbBasePath);
    if (!newDirectoryList.Status.IsSuccess()) {
        return restoreResult;
    }

    for (const auto& entry : newDirectoryList.Entries) {
        if (oldEntries.contains(entry.Name)) {
            continue;
        }

        const auto& fullPath = entry.Name; // RecursiveList returns full path instead of entry's name
        TMaybe<TStatus> result;

        switch (entry.Type) {
            case ESchemeEntryType::Directory: {
                result = NConsoleClient::RemoveDirectoryRecursive(SchemeClient, TableClient, nullptr, &QueryClient,
                    TString{fullPath}, ERecursiveRemovePrompt::Never, {}, true, false);
                break;
            }
            case ESchemeEntryType::Table: {
                result = TableClient.RetryOperationSync([&path = fullPath](TSession session) {
                    return session.DropTable(path).GetValueSync();
                });
                break;
            }
            case ESchemeEntryType::View: {
                result = QueryClient.RetryQuerySync([&path = fullPath](NQuery::TSession session) {
                    return session.ExecuteQuery(std::format("DROP VIEW IF EXISTS `{}`;", path),
                        NQuery::TTxControl::NoTx()).ExtractValueSync();
                });
                break;
            }
            default:
                break;
        }

        if (!result) {
            LOG_E("Error removing unexpected object: " << TString{fullPath}.Quote());
            return restoreResult;
        } else if (!result->IsSuccess()) {
            LOG_E("Error removing " << entry.Type << ": " << TString{fullPath}.Quote()
                << ": " << result->GetIssues().ToOneLineString());
            return restoreResult;
        }
    }

    return restoreResult;
}

TRestoreResult TRestoreClient::RestoreFolder(
        const TFsPath& fsPath,
        const TString& dbRestoreRoot,
        const TString& dbPathRelativeToRestoreRoot,
        const TRestoreSettings& settings,
        const THashSet<TString>& oldEntries)
{
    const TString dbPath = dbRestoreRoot + dbPathRelativeToRestoreRoot;

    LOG_D("Restore folder " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

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

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    const TString objectDbPath = Join('/', dbPath, fsPath.GetName());

    if (IsFileExists(fsPath.Child(NFiles::TableScheme().FileName))) {
        return RestoreTable(fsPath, objectDbPath, settings, oldEntries.contains(objectDbPath));
    }

    if (IsFileExists(fsPath.Child(NFiles::CreateView().FileName))) {
        // delay view restoration
        ViewRestorationCalls.emplace_back(fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings, oldEntries.contains(objectDbPath));
        return Result<TRestoreResult>();
    }

    if (IsFileExists(fsPath.Child(NFiles::CreateTopic().FileName))) {
        return RestoreTopic(fsPath, objectDbPath, settings, oldEntries.contains(objectDbPath));
    }

    if (IsFileExists(fsPath.Child(NFiles::Empty().FileName))) {
        return RestoreEmptyDir(fsPath, objectDbPath, settings, oldEntries.contains(objectDbPath));
    }

    TMaybe<TRestoreResult> result;

    TVector<TFsPath> children;
    fsPath.List(children);
    for (const auto& child : children) {
        const TString childDbPath = Join('/', dbPath, child.GetName());
        if (IsFileExists(child.Child(NFiles::TableScheme().FileName))) {
            result = RestoreTable(child, childDbPath, settings, oldEntries.contains(childDbPath));
        } else if (IsFileExists(child.Child(NFiles::Empty().FileName))) {
            result = RestoreEmptyDir(child, childDbPath, settings, oldEntries.contains(childDbPath));
        } else if (IsFileExists(child.Child(NFiles::CreateView().FileName))) {
            // delay view restoration
            ViewRestorationCalls.emplace_back(child, dbRestoreRoot, Join('/', dbPathRelativeToRestoreRoot, child.GetName()), settings, oldEntries.contains(childDbPath));
        } else if (IsFileExists(child.Child(NFiles::CreateTopic().FileName))) {
            result = RestoreTopic(child, childDbPath, settings, oldEntries.contains(childDbPath));
        } else if (child.IsDirectory()) {
            result = RestoreFolder(child, dbRestoreRoot, Join('/', dbPathRelativeToRestoreRoot, child.GetName()), settings, oldEntries);
        }

        if (result.Defined() && !result->IsSuccess()) {
            return *result;
        }
    }

    const bool dbPathExists = oldEntries.contains(dbPath);
    if (!result.Defined() && !dbPathExists) {
        // This situation occurs when all the children of the folder are views.
        return RestoreEmptyDir(fsPath, dbPath, settings, dbPathExists);
    }

    return RestorePermissions(fsPath, dbPath, settings, dbPathExists);
}

TRestoreResult TRestoreClient::RestoreView(
    const TFsPath& fsPath,
    const TString& dbRestoreRoot,
    const TString& dbPathRelativeToRestoreRoot,
    const TRestoreSettings& settings,
    bool isAlreadyExisting)
{
    LOG_D("Process " << fsPath.GetPath().Quote());

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    const TString dbPath = dbRestoreRoot + dbPathRelativeToRestoreRoot;
    LOG_I("Restore view " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    const auto createViewFile = fsPath.Child(NFiles::CreateView().FileName);
    TString query = TFileInput(createViewFile).ReadAll();

    NYql::TIssues issues;
    if (!RewriteCreateViewQuery(query, dbRestoreRoot, IsDatabase(SchemeClient, dbRestoreRoot), dbPath,
        createViewFile.GetPath().Quote(), issues
    )) {
        return Result<TRestoreResult>(dbPath, EStatus::BAD_REQUEST, issues.ToString());
    }

    if (settings.DryRun_) {
        return CheckExistenceAndType(SchemeClient, dbPath, ESchemeEntryType::View);
    }

    LOG_D("Executing view creation query: " << query.Quote());
    auto result = QueryClient.RetryQuerySync([&](NQuery::TSession session) {
        return session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    });

    if (result.IsSuccess()) {
        LOG_D("Created " << dbPath.Quote());
        return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
    }

    if (result.GetStatus() == EStatus::SCHEME_ERROR) {
        LOG_I("Failed to create " << dbPath.Quote() << ". Will retry.");
        // Scheme error happens when the view depends on a table (or a view) that is not yet restored.
        // Instead of tracking view dependencies, we simply retry the creation of the view later.
        ViewRestorationCalls.emplace_back(fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings, isAlreadyExisting);
    } else {
        LOG_E("Failed to create " << dbPath.Quote());
    }

    return Result<TRestoreResult>(dbPath, std::move(result));
}

TRestoreResult TRestoreClient::RestoreTopic(
    const TFsPath& fsPath,
    const TString& dbPath,
    const TRestoreSettings& settings,
    bool isAlreadyExisting)
{
    LOG_D("Process " << fsPath.GetPath().Quote());

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    LOG_I("Restore topic " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    if (settings.DryRun_) {
        return CheckExistenceAndType(SchemeClient, dbPath, ESchemeEntryType::Topic);
    }

    const auto creationRequest = ReadTopicCreationRequest(fsPath, Log.get());
    auto result = CreateTopic(TopicClient, dbPath, creationRequest);
    if (result.IsSuccess()) {
        LOG_D("Created " << dbPath.Quote());
        return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
    }

    LOG_E("Failed to create " << dbPath.Quote());
    return Result<TRestoreResult>(dbPath, std::move(result));
}

TRestoreResult TRestoreClient::RestoreTable(
        const TFsPath& fsPath,
        const TString& dbPath,
        const TRestoreSettings& settings,
        bool isAlreadyExisting)
{
    LOG_D("Process " << fsPath.GetPath().Quote());

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    auto scheme = ReadTableScheme(fsPath, Log.get());
    auto dumpedDesc = TableDescriptionFromProto(scheme);

    if (dumpedDesc.GetAttributes().contains(DOC_API_TABLE_VERSION_ATTR) && settings.SkipDocumentTables_) {
        LOG_I("Skip document table: " << fsPath.GetPath().Quote());
        return Result<TRestoreResult>();
    }

    if (settings.DryRun_) {
        return CheckSchema(dbPath, dumpedDesc);
    }

    LOG_I("Restore table " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    auto withoutIndexesDesc = TableDescriptionWithoutIndexesFromProto(scheme);
    auto createResult = TableClient.RetryOperationSync([&dbPath, &withoutIndexesDesc](TSession session) {
        return session.CreateTable(dbPath, TTableDescription(withoutIndexesDesc),
            TCreateTableSettings().RequestType(DOC_API_REQUEST_TYPE)).GetValueSync();
    });
    if (createResult.IsSuccess()) {
        LOG_D("Created " << dbPath.Quote());
    } else {
        LOG_E("Failed to create " << dbPath.Quote());
        return Result<TRestoreResult>(dbPath, std::move(createResult));
    }

    if (settings.RestoreData_) {
        auto result = RestoreData(fsPath, dbPath, settings, withoutIndexesDesc);
        if (!result.IsSuccess()) {
            return result;
        }
    } else {
        LOG_D("Skip restoring data of " << dbPath.Quote());
    }

    if (settings.RestoreIndexes_) {
        auto result = RestoreIndexes(dbPath, dumpedDesc);
        if (!result.IsSuccess()) {
            return result;
        }
    } else if (!scheme.indexes().empty()) {
        LOG_D("Skip restoring indexes of " << dbPath.Quote());
    }

    if (settings.RestoreChangefeeds_) {
        TVector<TFsPath> children;
        fsPath.List(children);
        for (const auto& fsChildPath : children) {
            const bool isChangefeedDir = IsFileExists(fsChildPath.Child(NFiles::Changefeed().FileName));
            if (isChangefeedDir) {
                auto result = RestoreChangefeeds(fsChildPath, dbPath);
                if (!result.IsSuccess()) {
                    return result;
                }
            }
        }
    } else {
        LOG_D("Skip restoring changefeeds of " << dbPath.Quote());
    }

    return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
}

TRestoreResult TRestoreClient::CheckSchema(const TString& dbPath, const TTableDescription& desc) {
    LOG_I("Check schema of " << dbPath.Quote());

    TMaybe<TTableDescription> actualDesc;
    auto descResult = DescribeTable(TableClient, dbPath, actualDesc);
    if (!descResult.IsSuccess()) {
        return Result<TRestoreResult>(dbPath, std::move(descResult));
    }

    auto unorderedColumns = [](const std::vector<TColumn>& orderedColumns) {
        THashMap<TString, TColumn> result;
        for (const auto& column : orderedColumns) {
            result.emplace(column.Name, column);
        }
        return result;
    };

    auto unorderedIndexes = [](const std::vector<TIndexDescription>& orderedIndexes) {
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

THolder<NPrivate::IDataWriter> TRestoreClient::CreateDataWriter(
        const TString& dbPath,
        const TRestoreSettings& settings,
        const TTableDescription& desc,
        const TVector<THolder<NPrivate::IDataAccumulator>>& accumulators)
{
    THolder<NPrivate::IDataWriter> writer;
    switch (settings.Mode_) {
        case TRestoreSettings::EMode::Yql:
        case TRestoreSettings::EMode::BulkUpsert: {
            // Need only one accumulator to initialize query string
            writer.Reset(CreateCompatWriter(dbPath, TableClient, accumulators[0].Get(), settings));
            break;
        }

        case TRestoreSettings::EMode::ImportData: {
            writer.Reset(CreateImportDataWriter(dbPath, desc, ImportClient, TableClient, accumulators, settings, Log));
            break;
        }
    }

    return writer;
}

TRestoreResult TRestoreClient::CreateDataAccumulators(
        TVector<THolder<NPrivate::IDataAccumulator>>& outAccumulators,
        const TString& dbPath,
        const TRestoreSettings& settings,
        const NTable::TTableDescription& desc,
        ui32 dataFilesCount)
{
    const ui32 accumulatorsCount = std::min(settings.InFly_, dataFilesCount);
    outAccumulators.resize(accumulatorsCount);

    switch (settings.Mode_) {
        case TRestoreSettings::EMode::Yql:
        case TRestoreSettings::EMode::BulkUpsert:
            for (size_t i = 0; i < accumulatorsCount; ++i) {
                outAccumulators[i].Reset(CreateCompatAccumulator(dbPath, desc, settings));
            }
            break;

        case TRestoreSettings::EMode::ImportData: {
            TMaybe<TTableDescription> actualDesc;
            auto descResult = DescribeTable(TableClient, dbPath, actualDesc);
            if (!descResult.IsSuccess()) {
                return Result<TRestoreResult>(dbPath, std::move(descResult));
            }
            for (size_t i = 0; i < accumulatorsCount; ++i) {
                outAccumulators[i].Reset(CreateImportDataAccumulator(desc, *actualDesc, settings, Log));
            }
            break;
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreData(
        const TFsPath& fsPath,
        const TString& dbPath,
        const TRestoreSettings& settings,
        const TTableDescription& desc)
{
    // Threads can access memory owned by this vector through pointers during restore operation
    TVector<TFsPath> dataFiles = CollectDataFiles(fsPath);

    const ui32 dataFilesCount = dataFiles.size();
    if (dataFilesCount == 0) {
        return Result<TRestoreResult>();
    }

    TVector<THolder<NPrivate::IDataAccumulator>> accumulators;
    if (auto res = CreateDataAccumulators(accumulators, dbPath, settings, desc, dataFilesCount); !res.IsSuccess()) {
        return res;
    }

    THolder<NPrivate::IDataWriter> writer = CreateDataWriter(dbPath, settings, desc, accumulators);

    TVector<TRestoreResult> accumulatorWorkersResults(accumulators.size(), Result<TRestoreResult>());
    TThreadPool accumulatorWorkers(TThreadPool::TParams().SetBlocking(true));
    accumulatorWorkers.Start(accumulators.size(), accumulators.size());

    const ui32 dataFilesPerAccumulator = dataFilesCount / accumulators.size();
    const ui32 dataFilesPerAccumulatorRemainder = dataFilesCount % accumulators.size();
    for (ui32 i = 0; i < accumulators.size(); ++i) {
        auto* accumulator = accumulators[i].Get();

        ui32 dataFileIdStart = dataFilesPerAccumulator * i + std::min(i, dataFilesPerAccumulatorRemainder);
        ui32 dataFileIdEnd = dataFilesPerAccumulator * (i + 1) + std::min(i + 1, dataFilesPerAccumulatorRemainder);
        auto func = [&, i, dataFileIdStart, dataFileIdEnd, accumulator]() {
            for (size_t id = dataFileIdStart; id < dataFileIdEnd; ++id) {
                const TFsPath& dataFile = dataFiles[id];

                LOG_D("Read data from " << dataFile.GetPath().Quote());

                TFileInput input(dataFile, settings.FileBufferSize_);
                TString line;
                ui64 lineNo = 0;

                while (input.ReadLine(line)) {
                    auto l = NPrivate::TLine(std::move(line), dataFile.GetPath(), ++lineNo);

                    for (auto status = accumulator->Check(l); status != NPrivate::IDataAccumulator::OK; status = accumulator->Check(l)) {
                        if (status == NPrivate::IDataAccumulator::ERROR) {
                            accumulatorWorkersResults[i] = Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR,
                                TStringBuilder() << "Invalid data: " << l.GetLocation());
                            return;
                        }

                        if (!accumulator->Ready(true)) {
                            LOG_E("Error reading data from " << dataFile.GetPath().Quote());
                            accumulatorWorkersResults[i] = Result<TRestoreResult>(dbPath, EStatus::INTERNAL_ERROR, "Data is not ready");
                            return;
                        }

                        if (!writer->Push(accumulator->GetData(true))) {
                            LOG_E("Error writing data to " << dbPath.Quote() << ", file: " << dataFile.GetPath().Quote());
                            accumulatorWorkersResults[i] = Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #1");
                            return;
                        }
                    }

                    accumulator->Feed(std::move(l));
                    if (accumulator->Ready()) {
                        if (!writer->Push(accumulator->GetData())) {
                            LOG_E("Error writing data to " << dbPath.Quote() << ", file: " << dataFile.GetPath().Quote());
                            accumulatorWorkersResults[i] = Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #2");
                            return;
                        }
                    }
                }
            }

            while (accumulator->Ready(true)) {
                if (!writer->Push(accumulator->GetData(true))) {
                    accumulatorWorkersResults[i] = Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #3");
                    return;
                }
            }
        };

        if (!accumulatorWorkers.AddFunc(std::move(func))) {
            return Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Can't start restoring data: queue is full or shutting down");
        }
    }

    accumulatorWorkers.Stop();
    if (auto res = CombineResults(accumulatorWorkersResults); !res.IsSuccess()) {
        return res;
    }

    // ensure that all data is restored
    while (true) {
        writer->Wait();

        bool dataFound = false;
        for (auto& acc : accumulators) {
            if (acc->Ready(true)) {
                dataFound = true;
                break;
            }
        }

        if (dataFound) {
            writer = CreateDataWriter(dbPath, settings, desc, accumulators);
            for (auto& acc : accumulators) {
                while (acc->Ready(true)) {
                    if (!writer->Push(acc->GetData(true))) {
                        return Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #4");
                    }
                }
            }
        } else {
            break;
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreIndexes(const TString& dbPath, const TTableDescription& desc) {
    TMaybe<TTableDescription> actualDesc;
    auto descResult = DescribeTable(TableClient, dbPath, actualDesc);
    if (!descResult.IsSuccess()) {
        return Result<TRestoreResult>(dbPath, std::move(descResult));
    }

    for (const auto& index : desc.GetIndexDescriptions()) {
        if (FindPtr(actualDesc->GetIndexDescriptions(), index)) {
            continue;
        }

        LOG_D("Restore index " << TString{index.GetIndexName()}.Quote() << " on " << dbPath.Quote());

        TOperation::TOperationId buildIndexId;
        auto buildIndexStatus = TableClient.RetryOperationSync([&, &outId = buildIndexId](TSession session) {
            auto settings = TAlterTableSettings().AppendAddIndexes(index);
            auto result = session.AlterTableLong(dbPath, settings).GetValueSync();
            if (IsOperationStarted(result.Status())) {
                outId = result.Id();
            }
            return result.Status();
        });

        if (!IsOperationStarted(buildIndexStatus)) {
            LOG_E("Error building index " << TString{index.GetIndexName()}.Quote() << " on " << dbPath.Quote());
            return Result<TRestoreResult>(dbPath, std::move(buildIndexStatus));
        }

        auto waitForIndexBuildStatus = WaitForIndexBuild(OperationClient, buildIndexId);
        if (!waitForIndexBuildStatus.IsSuccess()) {
            LOG_E("Error building index " << TString{index.GetIndexName()}.Quote() << " on " << dbPath.Quote());
            return Result<TRestoreResult>(dbPath, std::move(waitForIndexBuildStatus));
        }

        auto forgetStatus = NConsoleClient::RetryFunction([&]() {
            return OperationClient.Forget(buildIndexId).GetValueSync();
        });
        if (!forgetStatus.IsSuccess()) {
            LOG_E("Error building index " << TString{index.GetIndexName()}.Quote() << " on " << dbPath.Quote());
            return Result<TRestoreResult>(dbPath, std::move(forgetStatus));
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreChangefeeds(const TFsPath& fsPath, const TString& dbPath) {
    LOG_D("Process " << fsPath.GetPath().Quote());
    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    auto changefeedProto = ReadChangefeedDescription(fsPath, Log.get());
    auto topicProto = ReadTopicDescription(fsPath, Log.get());

    auto changefeedDesc = ChangefeedDescriptionFromProto(changefeedProto);
    auto topicDesc = TopicDescriptionFromProto(std::move(topicProto));

    changefeedDesc = changefeedDesc.WithRetentionPeriod(topicDesc.GetRetentionPeriod());

    auto result = TableClient.RetryOperationSync([&changefeedDesc, &dbPath](TSession session) {
        return session.AlterTable(dbPath, TAlterTableSettings().AppendAddChangefeeds(changefeedDesc)).GetValueSync();
    });
    if (result.IsSuccess()) {
        LOG_D("Created " << fsPath.GetPath().Quote());
    } else {
        LOG_E("Failed to create " << fsPath.GetPath().Quote());
        return Result<TRestoreResult>(fsPath.GetPath(), std::move(result));
    }

    return RestoreConsumers(Join("/", dbPath, fsPath.GetName()), topicDesc.GetConsumers());;
}

TRestoreResult TRestoreClient::RestoreConsumers(const TString& topicPath, const std::vector<TConsumer>& consumers) {
    for (const auto& consumer : consumers) {
        auto result = TopicClient.AlterTopic(topicPath,
            TAlterTopicSettings()
                .BeginAddConsumer()
                    .ConsumerName(consumer.GetConsumerName())
                    .Important(consumer.GetImportant())
                    .Attributes(consumer.GetAttributes())
                .EndAddConsumer()
        ).GetValueSync();
        if (result.IsSuccess()) {
            LOG_D("Created consumer " << TString{consumer.GetConsumerName()}.Quote() << " for " << topicPath.Quote());
        } else {
            LOG_E("Failed to create " << TString{consumer.GetConsumerName()}.Quote() << " for " << topicPath.Quote());
            return Result<TRestoreResult>(topicPath, std::move(result));
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestorePermissions(
        const TFsPath& fsPath,
        const TString& dbPath,
        const TRestoreSettings& settings,
        bool isAlreadyExisting)
{
    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    if (!settings.RestoreACL_) {
        return Result<TRestoreResult>();
    }

    if (isAlreadyExisting) {
        return Result<TRestoreResult>();
    }

    if (!fsPath.Child(NFiles::Permissions().FileName).Exists()) {
        return Result<TRestoreResult>();
    }

    LOG_D("Restore ACL " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    auto permissions = ReadPermissions(fsPath, Log.get());
    return ModifyPermissions(SchemeClient, dbPath, TModifyPermissionsSettings(permissions));
}

TRestoreResult TRestoreClient::RestoreEmptyDir(
        const TFsPath& fsPath,
        const TString& dbPath,
        const TRestoreSettings& settings,
        bool isAlreadyExisting)
{
    LOG_D("Process " << fsPath.GetPath().Quote());

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    LOG_I("Restore empty directory " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    auto result = MakeDirectory(SchemeClient, dbPath);
    if (!result.IsSuccess()) {
        return result;
    }

    return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
}

} // NYdb::NDump

Y_DECLARE_OUT_SPEC(, NYdb::NDump::NPrivate::TLocation, o, x) {
    return x.Out(o);
}
