#include "restore_compat.h"
#include "restore_impl.h"
#include "restore_import_data.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/common/recursive_list.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/lib/ydb_cli/dump/util/log.h>
#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/system/hp_timer.h>
#include <util/system/info.h>

#include <google/protobuf/text_format.h>

#include <format>

namespace NYdb::NDump {

using namespace NCms;
using namespace NConsoleClient;
using namespace NImport;
using namespace NOperation;
using namespace NRateLimiter;
using namespace NScheme;
using namespace NTable;
using namespace NTopic;
using namespace NThreading;

extern const char DOC_API_TABLE_VERSION_ATTR[] = "__document_api_version";
extern const char DOC_API_REQUEST_TYPE[] = "_document_api_request";

namespace {

bool IsFileExists(const TFsPath& path) {
    return path.Exists() && path.IsFile();
}

TString ReadFromFile(const TFsPath& fsDirPath, const TLog* log, const NFiles::TFileInfo& fileInfo) {
    const auto fsPath = fsDirPath.Child(fileInfo.FileName);
    LOG_IMPL(log, ELogPriority::TLOG_DEBUG, "Read " << fileInfo.LogObjectType << " from " << fsPath.GetPath().Quote());
    return TFileInput(fsPath).ReadAll();
}

TString ReadViewQuery(const TFsPath& fsDirPath, const TLog* log) {
    return ReadFromFile(fsDirPath, log, NFiles::CreateView());
}

TString ReadAsyncReplicationQuery(const TFsPath& fsDirPath, const TLog* log) {
    return ReadFromFile(fsDirPath, log, NFiles::CreateAsyncReplication());
}

TString ReadExternalDataSourceQuery(const TFsPath& fsDirPath, const TLog* log) {
    return ReadFromFile(fsDirPath, log, NFiles::CreateExternalDataSource());
}

TString ReadExternalTableQuery(const TFsPath& fsDirPath, const TLog* log) {
    return ReadFromFile(fsDirPath, log, NFiles::CreateExternalTable());
}

template <typename TProtoType>
TProtoType ReadProtoFromFile(const TFsPath& fsDirPath, const TLog* log, const NFiles::TFileInfo& fileInfo) {
    TProtoType proto;
    Y_ENSURE(google::protobuf::TextFormat::ParseFromString(ReadFromFile(fsDirPath, log, fileInfo), &proto));
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

Ydb::Coordination::CreateNodeRequest ReadCoordinationNodeCreationRequest(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::Coordination::CreateNodeRequest>(fsDirPath, log, NDump::NFiles::CreateCoordinationNode());
}

Ydb::RateLimiter::CreateResourceRequest ReadRateLimiterCreationRequest(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::RateLimiter::CreateResourceRequest>(fsDirPath, log, NDump::NFiles::CreateRateLimiter());
}

Ydb::Scheme::ModifyPermissionsRequest ReadPermissions(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::Scheme::ModifyPermissionsRequest>(fsDirPath, log, NFiles::Permissions());
}

Ydb::Cms::CreateDatabaseRequest ReadDatabaseDescription(const TFsPath& fsDirPath, const TLog* log) {
    return ReadProtoFromFile<Ydb::Cms::CreateDatabaseRequest>(fsDirPath, log, NFiles::Database());
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
        ExponentialBackoff(retrySleep, TDuration::Minutes(1));
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

TRestoreResult CombineResults(const TVector<TFuture<TRestoreResult>>& results) {
    try {
        for (auto result : results) {
            auto status = result.ExtractValueSync();
            if (!status.IsSuccess()) {
                return status;
            }
        }
    } catch (NStatusHelpers::TYdbErrorException& e) {
        return e.ExtractStatus();
    } catch (const std::exception& e) {
        return Result<TRestoreResult>(EStatus::INTERNAL_ERROR,
            TStringBuilder() << "Caught exception: " << e.what()
        );
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
    auto result = RetryFunction([&]() {
        return client.CreateTopic(dbPath, settings).ExtractValueSync();
    });
    return result;
}

TStatus CreateCoordinationNode(
    NCoordination::TClient& client,
    const TString& dbPath,
    const Ydb::Coordination::CreateNodeRequest& request)
{
    const auto settings = NCoordination::TCreateNodeSettings(request.config());
    auto result = RetryFunction([&]() {
        return client.CreateNode(dbPath, settings).ExtractValueSync();
    });
    return result;
}

TStatus CreateRateLimiter(
    TRateLimiterClient& client,
    const TString& coordinationNodePath,
    const TString& rateLimiterPath,
    const Ydb::RateLimiter::CreateResourceRequest& request)
{
    const auto settings = TCreateResourceSettings(request);
    auto result = RetryFunction([&]() {
        return client.CreateResource(coordinationNodePath, rateLimiterPath, settings).ExtractValueSync();
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

TDelayedRestoreCall::TDelayedRestoreCall(
    ESchemeEntryType type,
    TFsPath fsPath,
    TString dbPath,
    TRestoreSettings settings,
    bool isAlreadyExisting
)
    : Type(type)
    , FsPath(fsPath)
    , DbPath(dbPath)
    , Settings(settings)
    , IsAlreadyExisting(isAlreadyExisting)
{}

TDelayedRestoreCall::TDelayedRestoreCall(
    ESchemeEntryType type,
    TFsPath fsPath,
    TString dbRestoreRoot,
    TString dbPathRelativeToRestoreRoot,
    TRestoreSettings settings,
    bool isAlreadyExisting
)
    : Type(type)
    , FsPath(fsPath)
    , DbPath(TTwoComponentPath(dbRestoreRoot, dbPathRelativeToRestoreRoot))
    , Settings(settings)
    , IsAlreadyExisting(isAlreadyExisting)
{}

int TDelayedRestoreCall::GetOrder() const {
    switch (Type) {
        case ESchemeEntryType::View:
            return std::numeric_limits<int>::max();
        default:
            return 0;
    }
}

auto operator<=>(const TDelayedRestoreCall& lhs, const TDelayedRestoreCall& rhs) {
    return lhs.GetOrder() <=> rhs.GetOrder();
}

TRestoreResult TDelayedRestoreManager::Restore(const TDelayedRestoreCall& call) {
    switch (call.Type) {
        case ESchemeEntryType::View: {
            const auto& [dbRestoreRoot, dbPathRelativeToRestoreRoot] = std::get<TDelayedRestoreCall::TTwoComponentPath>(call.DbPath);
            return Client->RestoreView(call.FsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, call.Settings, call.IsAlreadyExisting);
        }
        case ESchemeEntryType::ExternalTable: {
            const auto& dbPath = std::get<TDelayedRestoreCall::TSimplePath>(call.DbPath);
            return Client->RestoreExternalTable(call.FsPath, dbPath, call.Settings, call.IsAlreadyExisting);
        }
        default:
            ythrow TBadArgumentException() << "Attempting to restore an unexpected object from: " << call.FsPath;
    }
}

bool TDelayedRestoreManager::ShouldRetry(const TRestoreResult& result, ESchemeEntryType type) {
    switch (type) {
        case ESchemeEntryType::View:
            return result.GetStatus() == EStatus::SCHEME_ERROR;
        default:
            return false;
    }
}

TRestoreResult TDelayedRestoreManager::RestoreWithRetries(TVector<TDelayedRestoreCall>&& callsToRetry) {
    bool stopRetries = false;
    TVector<TDelayedRestoreCall> nextRound;
    while (!callsToRetry.empty()) {
        nextRound.clear();
        for (const auto& call : callsToRetry) {
            auto result = Restore(call);
            if (!result.IsSuccess()) {
                if (stopRetries || !ShouldRetry(result, call.Type)) {
                    return result;
                }
                nextRound.emplace_back(call);
            }
        }
        // errors are persistent
        stopRetries = nextRound.size() == callsToRetry.size();
        std::swap(nextRound, callsToRetry);
    }
    return Result<TRestoreResult>();
}

void TDelayedRestoreManager::SetClient(TRestoreClient& client) {
    Client = &client;
}

TRestoreResult TDelayedRestoreManager::RestoreDelayed() {
    std::sort(Calls.begin(), Calls.end());
    TVector<TDelayedRestoreCall> callsToRetry;
    for (const auto& call : Calls) {
        auto result = Restore(call);
        if (!result.IsSuccess()) {
            if (!ShouldRetry(result, call.Type)) {
                return result;
            }
            callsToRetry.emplace_back(call);
        }
    }
    return RestoreWithRetries(std::move(callsToRetry));
}

} // NPrivate

TRestoreClient::TRestoreClient(const TDriver& driver, const std::shared_ptr<TLog>& log)
    : ImportClient(driver)
    , OperationClient(driver)
    , SchemeClient(driver)
    , TableClient(driver)
    , TopicClient(driver)
    , CoordinationNodeClient(driver)
    , RateLimiterClient(driver)
    , QueryClient(driver)
    , CmsClient(driver)
    , Log(log)
    , DriverConfig(driver.GetConfig())
{
    DelayedRestoreManager.SetClient(*this);
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
            return Result<TRestoreResult>(dbBasePath, EStatus::SCHEME_ERROR, "Can not find existing path");
        }

        dbBasePath = dbBasePath.Parent();
    }

    LOG_D("Resolved db base path: " << dbBasePath.GetPath().Quote());

    auto oldDirectoryList = RecursiveList(SchemeClient, dbBasePath);
    if (const auto& status = oldDirectoryList.Status; !status.IsSuccess()) {
        LOG_E("Error listing db base path: " << dbBasePath.GetPath().Quote() << ": " << status.GetIssues().ToOneLineString());
        return Result<TRestoreResult>(dbBasePath, EStatus::SCHEME_ERROR, "Can not list existing directory");
    }

    THashSet<TString> oldEntries;
    for (const auto& entry : oldDirectoryList.Entries) {
        oldEntries.insert(TString{entry.Name});
    }

    // restore
    auto restoreResult = RestoreFolder(fsPath, dbPath, "", settings, oldEntries);
    if (auto result = DelayedRestoreManager.RestoreDelayed(); !result.IsSuccess()) {
        restoreResult = result;
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

    TVector<const TSchemeEntry*> entriesToDropInSecondPass;

    for (const auto& entry : newDirectoryList.Entries) {
        if (oldEntries.contains(entry.Name)) {
            continue;
        }

        const auto& fullPath = entry.Name; // RecursiveList returns full path instead of entry's name
        TMaybe<TStatus> result;

        switch (entry.Type) {
            case ESchemeEntryType::Directory:
                result = RemoveDirectoryRecursive(SchemeClient, TableClient, nullptr, &QueryClient,
                    TString{fullPath}, ERecursiveRemovePrompt::Never, {}, true, false);
                break;
            case ESchemeEntryType::Table:
                result = TableClient.RetryOperationSync([&path = fullPath](TSession session) {
                    return session.DropTable(path).GetValueSync();
                });
                break;
            case ESchemeEntryType::View:
                result = QueryClient.RetryQuerySync([&path = fullPath](NQuery::TSession session) {
                    return session.ExecuteQuery(std::format("DROP VIEW `{}`;", path),
                        NQuery::TTxControl::NoTx()).ExtractValueSync();
                });
                break;
            case ESchemeEntryType::Topic:
                result = RetryFunction([&client = TopicClient, &path = fullPath]() {
                    return client.DropTopic(path).ExtractValueSync();
                });
                break;
            case ESchemeEntryType::CoordinationNode:
                result = RetryFunction([&client = CoordinationNodeClient, &path = fullPath]() {
                    return client.DropNode(path).ExtractValueSync();
                });
                break;
            case ESchemeEntryType::ExternalDataSource:
                entriesToDropInSecondPass.emplace_back(&entry);
                continue;
            case ESchemeEntryType::ExternalTable:
                result = QueryClient.RetryQuerySync([&path = fullPath](NQuery::TSession session) {
                    return session.ExecuteQuery(std::format("DROP EXTERNAL TABLE `{}`;", path),
                        NQuery::TTxControl::NoTx()).ExtractValueSync();
                });
                break;
            default:
                break;
        }

        if (!result) {
            LOG_E("Error removing unexpected object: " << TString{fullPath}.Quote());
            return restoreResult;
        } else if (!result->IsSuccess()) {
            LOG_E("Error removing " << entry.Type << ": " << TString{fullPath}.Quote()
                << ", issues: " << result->GetIssues().ToOneLineString());
            return restoreResult;
        }
    }

    for (const auto* entry : entriesToDropInSecondPass) {
        TMaybe<TStatus> result;
        switch (entry->Type) {
            case ESchemeEntryType::ExternalDataSource:
                result = QueryClient.RetryQuerySync([&path = entry->Name](NQuery::TSession session) {
                    return session.ExecuteQuery(std::format("DROP EXTERNAL DATA SOURCE `{}`;", path),
                        NQuery::TTxControl::NoTx()).ExtractValueSync();
                });
                break;
            default:
                break;
        }
        Y_ENSURE(result, "Unexpected entry to drop in the second pass");
        if (!result->IsSuccess()) {
            LOG_E("Error removing " << entry->Type << ": " << TString{entry->Name}.Quote()
                << ", issues: " << result->GetIssues().ToOneLineString());
            return restoreResult;
        }
    }

    return restoreResult;
}

TRestoreResult TRestoreClient::FindClusterRootPath() {
    if (!ClusterRootPath.empty()) {
        return Result<TRestoreResult>();
    }

    LOG_D("Try to find cluster root path");

    auto status = NDump::ListDirectory(SchemeClient, "/");
    if (!status.IsSuccess()) {
        LOG_E("Error finding cluster root path: " << status.GetIssues().ToOneLineString());
        return status;
    }

    if (status.GetChildren().size() != 1) {
        return Result<TRestoreResult>(EStatus::PRECONDITION_FAILED,
            TStringBuilder() << "Exactly one cluster root expected, found: " << JoinSeq(", ", status.GetChildren()));
    }

    ClusterRootPath = "/" + status.GetChildren().begin()->Name;

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreClusterRoot(const TFsPath& fsPath) {
    if (auto result = FindClusterRootPath(); !result.IsSuccess()) {
        return result;
    }

    LOG_I("Restore cluster root " << ClusterRootPath.Quote() << " from " << fsPath.GetPath().Quote());

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

    TDriverConfig rootDriverConfig(DriverConfig);
    rootDriverConfig.SetDatabase(ClusterRootPath);
    TDriver rootDriver(rootDriverConfig);
    TSchemeClient rootSchemeClient(rootDriver);
    TTableClient rootTableClient(rootDriver);

    if (auto result = RestoreUsers(rootTableClient, fsPath, ClusterRootPath); !result.IsSuccess()) {
        return result;
    }

    if (auto result = RestoreGroups(rootTableClient, fsPath, ClusterRootPath); !result.IsSuccess()) {
        return result;
    }

    if (auto result = RestoreGroupMembers(rootTableClient, fsPath, ClusterRootPath); !result.IsSuccess()) {
        return result;
    }

    if (auto result = RestorePermissionsImpl(rootSchemeClient, fsPath, ClusterRootPath); !result.IsSuccess()) {
        return result;
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::WaitForAvailableNodes(const TString& database, TDuration waitDuration) {
    TDriverConfig dbDriverConfig = DriverConfig;
    dbDriverConfig.SetDatabase(database);

    THPTimer timer;

    NDiscovery::TDiscoveryClient client(dbDriverConfig);
    TDuration retrySleep = TDuration::MilliSeconds(1000);
    while (true) {
        auto result = client.ListEndpoints().GetValueSync();
        if (result.GetStatus() == EStatus::UNAVAILABLE) {
            auto timeSpent = TDuration::Seconds(timer.Passed());
            if (timeSpent > waitDuration) {
                auto error = TStringBuilder()
                    << "Timeout waiting for available database nodes for " << database.Quote()
                    << " for " << waitDuration
                    << ", make sure that nodes are running and restart restore";
                LOG_E(error);
                return Result<TRestoreResult>(EStatus::TIMEOUT, error);
            }

            auto timeLeft = waitDuration - timeSpent;
            LOG_I("Waiting for available database nodes for " << database.Quote()
                << ", make sure that nodes are running"
                << ", time left: " << timeLeft);
            retrySleep = Min(retrySleep, timeLeft);
            ExponentialBackoff(retrySleep, TDuration::Minutes(1));
        } else {
            return result;
        }
    }
}

TRestoreResult TRestoreClient::RestoreUsers(TTableClient& client, const TFsPath& fsPath, const TString& dbPath) {
    LOG_D("Restore users to " << dbPath.Quote());

    const auto createUserPath = fsPath.Child(NFiles::CreateUser().FileName);
    auto query = TFileInput(createUserPath).ReadAll();

    TVector<TString> statements;
    Split(query, "\n", statements);
    for (const auto& statement : statements) {
        auto statementResult = client.RetryOperationSync([&](TSession session) {
            return session.ExecuteSchemeQuery(statement).ExtractValueSync();
        });

        if (statement.StartsWith("CREATE")
            && statementResult.GetStatus() == EStatus::PRECONDITION_FAILED
            && statementResult.GetIssues().ToOneLineString().find("exists") != TString::npos)
        {
            LOG_D("User from create statement " << statement.Quote() << " already exists, trying to alter it");
            auto alterStatement = "ALTER" + statement.substr(6);
            auto alterStatementResult = client.RetryOperationSync([&](TSession session) {
                return session.ExecuteSchemeQuery(alterStatement).ExtractValueSync();
            });
            if (!alterStatementResult.IsSuccess()) {
                LOG_E("Failed to execute statement for restoring user: "
                    << alterStatement.Quote() << ", error: "
                    << alterStatementResult.GetIssues().ToOneLineString());
                return alterStatementResult;
            }
        } else if (statementResult.GetStatus() == EStatus::UNAUTHORIZED) {
            LOG_W("Not enough rights to restore user from statement " << statement.Quote() << ", skipping");
            continue;
        } else if (!statementResult.IsSuccess()) {
            LOG_E("Failed to execute statement for restoring user: "
                << statement.Quote() << ", error: "
                << statementResult.GetIssues().ToOneLineString());
            return statementResult;
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreGroups(TTableClient& client, const TFsPath& fsPath, const TString& dbPath) {
    LOG_D("Restore groups to " << dbPath.Quote());

    const auto createGroupPath = fsPath.Child(NFiles::CreateGroup().FileName);
    auto query = TFileInput(createGroupPath).ReadAll();

    TVector<TString> statements;
    Split(query, "\n", statements);
    for (const auto& statement : statements) {
        auto statementResult = client.RetryOperationSync([&](TSession session) {
            return session.ExecuteSchemeQuery(statement).ExtractValueSync();
        });

        if (statementResult.GetStatus() == EStatus::PRECONDITION_FAILED
            && statementResult.GetIssues().ToOneLineString().find("exists") != TString::npos) {
            LOG_D("Group from create statement " << statement.Quote() << " already exists, skipping");
            continue;
        } else if (statementResult.GetStatus() == EStatus::UNAUTHORIZED) {
            LOG_W("Not enough rights to restore group from statement " << statement.Quote() << ", skipping");
            continue;
        } else if (!statementResult.IsSuccess()) {
            LOG_E("Failed to execute statement for restoring group: "
                << statement.Quote() << ", error: "
                << statementResult.GetIssues().ToOneLineString());
            return statementResult;
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreGroupMembers(TTableClient& client, const TFsPath& fsPath, const TString& dbPath) {
    LOG_D("Restore group members to " << dbPath.Quote());

    const auto alterGroupPath = fsPath.Child(NFiles::AlterGroup().FileName);
    auto query = TFileInput(alterGroupPath).ReadAll();

    TVector<TString> statements;
    Split(query, "\n", statements);
    for (const auto& statement : statements) {
        auto statementResult = client.RetryOperationSync([&](TSession session) {
            return session.ExecuteSchemeQuery(statement).ExtractValueSync();
        });

        if (statementResult.GetStatus() == EStatus::UNAUTHORIZED) {
            LOG_W("Not enough rights to restore group member from statement " << statement.Quote() << ", skipping");
            continue;
        } else if (!statementResult.IsSuccess()) {
            LOG_E("Failed to execute statement for restoring group members: "
                << statement.Quote() << ", error: "
                << statementResult.GetIssues().ToOneLineString());
            return statementResult;
        }
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::ReplaceClusterRoot(TString& outPath) {
    if (auto result = FindClusterRootPath(); !result.IsSuccess()) {
        return result;
    }

    size_t clusterRootEnd = outPath.find('/', 1);
    if (clusterRootEnd != std::string::npos) {
        outPath = ClusterRootPath + outPath.substr(clusterRootEnd);
    } else {
        return Result<TRestoreResult>(EStatus::INTERNAL_ERROR,
            TStringBuilder() << "Can't find cluster root path in "
            << outPath.Quote() << " to replace it on "
            << ClusterRootPath.Quote());
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreDatabaseImpl(const TString& fsPath, const TRestoreDatabaseSettings& settings) {
    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    auto dbDesc = ReadDatabaseDescription(fsPath, Log.get());

    TString dbPath;
    if (settings.Database_.has_value()) {
        dbPath = *settings.Database_;
    } else {
        // Get database path from dump and adjust it to the cluster
        dbPath = dbDesc.path();
        if (auto result = ReplaceClusterRoot(dbPath); !result.IsSuccess()) {
            return result;
        }
    }

    LOG_I("Restore database from " << fsPath.Quote() << " to " << dbPath.Quote());

    if (auto result = CreateDatabase(CmsClient, dbPath, TCreateDatabaseSettings(dbDesc)); !result.IsSuccess()) {
        if (result.GetStatus() == EStatus::ALREADY_EXISTS) {
            LOG_W("Database " << dbPath.Quote() << " already exists, continue restoring to this database");
        } else if (result.GetStatus() == EStatus::UNAUTHORIZED) {
            LOG_W("Not enough rights to create database " << dbPath.Quote() << ", try to restore to existing database");
        } else {
            return result;
        }
    }

    if (auto result = WaitForAvailableNodes(dbPath, settings.WaitNodesDuration_); !result.IsSuccess()) {
        return result;
    }

    TDriverConfig dbDriverConfig(DriverConfig);
    dbDriverConfig.SetDatabase(dbPath);
    TDriver dbDriver(dbDriverConfig);
    TSchemeClient dbSchemeClient(dbDriver);
    TTableClient dbTableClient(dbDriver);

    if (auto result = RestoreUsers(dbTableClient, fsPath, dbPath); !result.IsSuccess()) {
        return result;
    }

    if (auto result = RestoreGroups(dbTableClient, fsPath, dbPath); !result.IsSuccess()) {
        return result;
    }

    if (auto result = RestoreGroupMembers(dbTableClient, fsPath, dbPath); !result.IsSuccess()) {
        return result;
    }

    if (auto result = RestorePermissionsImpl(dbSchemeClient, fsPath, dbPath); !result.IsSuccess()) {
        return result;
    }

    if (settings.WithContent_) {
        auto restoreResult = RestoreFolder(fsPath, dbPath, "", {}, {});
        if (auto result = DelayedRestoreManager.RestoreDelayed(); !result.IsSuccess()) {
            restoreResult = result;
        }
        return restoreResult;
    } else {
        return Result<TRestoreResult>();
    }
}

TRestoreResult TRestoreClient::RestoreDatabase(const TString& fsPath, const TRestoreDatabaseSettings& settings) {
    auto result = RestoreDatabaseImpl(fsPath, settings);
    if (result.IsSuccess()) {
        LOG_I("Restore database completed successfully");
    } else {
        LOG_E("Restore database failed: " << result.GetIssues().ToOneLineString());
    }

    return result;
}

TRestoreResult TRestoreClient::RestoreDatabases(const TFsPath& fsPath, const TRestoreClusterSettings& settings) {
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

    if (IsFileExists(fsPath.Child(NFiles::Database().FileName))) {
        TRestoreDatabaseSettings dbSettings = {
            .WaitNodesDuration_ = settings.WaitNodesDuration_,
            .Database_ = std::nullopt,
            .WithContent_ = false
        };

        if (auto result = RestoreDatabaseImpl(fsPath, dbSettings); !result.IsSuccess()) {
            return result;
        }
    }

    TVector<TFsPath> children;
    fsPath.List(children);

    EStatus statusCode = EStatus::SUCCESS;
    NIssue::TIssues issues;

    for (const auto& child : children) {
        if (child.IsDirectory()) {
            if (auto result = RestoreDatabases(child, settings); !result.IsSuccess()) {
                // don't abort, try to restore all databases
                issues.AddIssues(result.GetIssues());
                statusCode = EStatus::GENERIC_ERROR;
            }
        }
    }

    return TStatus(statusCode, std::move(issues));
}

TRestoreResult TRestoreClient::RestoreClusterImpl(const TString& fsPath, const TRestoreClusterSettings& settings) {
    if (auto result = RestoreClusterRoot(fsPath); !result.IsSuccess()) {
        return result;
    }

    if (auto result = RestoreDatabases(fsPath, settings); !result.IsSuccess()) {
        return result;
    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreCluster(const TString& fsPath, const TRestoreClusterSettings& settings) {
    LOG_I("Restore cluster from " << fsPath.Quote());

    auto result = RestoreClusterImpl(fsPath, settings);
    if (result.IsSuccess()) {
        LOG_I("Restore cluster completed successfully");
    } else {
        LOG_E("Restore cluster failed: " << result.GetIssues().ToOneLineString());
    }

    return result;
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
        DelayedRestoreManager.Add(ESchemeEntryType::View, fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings, oldEntries.contains(objectDbPath));
        return Result<TRestoreResult>();
    }

    if (IsFileExists(fsPath.Child(NFiles::CreateTopic().FileName))) {
        return RestoreTopic(fsPath, objectDbPath, settings, oldEntries.contains(objectDbPath));
    }

    if (IsFileExists(fsPath.Child(NFiles::CreateCoordinationNode().FileName))) {
        return RestoreCoordinationNode(fsPath, objectDbPath, settings, oldEntries.contains(objectDbPath));
    }

    if (IsFileExists(fsPath.Child(NFiles::CreateAsyncReplication().FileName))) {
        return RestoreReplication(fsPath, dbRestoreRoot, dbPathRelativeToRestoreRoot, settings, oldEntries.contains(objectDbPath));
    }

    if (IsFileExists(fsPath.Child(NFiles::CreateExternalDataSource().FileName))) {
        return RestoreExternalDataSource(fsPath, objectDbPath, settings, oldEntries.contains(objectDbPath));
    }

    if (IsFileExists(fsPath.Child(NFiles::CreateExternalTable().FileName))) {
        DelayedRestoreManager.Add(ESchemeEntryType::ExternalTable, fsPath, objectDbPath, settings, oldEntries.contains(objectDbPath));
        return Result<TRestoreResult>();
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
            DelayedRestoreManager.Add(ESchemeEntryType::View, child, dbRestoreRoot, Join('/', dbPathRelativeToRestoreRoot, child.GetName()), settings, oldEntries.contains(childDbPath));
        } else if (IsFileExists(child.Child(NFiles::CreateTopic().FileName))) {
            result = RestoreTopic(child, childDbPath, settings, oldEntries.contains(childDbPath));
        } else if (IsFileExists(child.Child(NFiles::CreateCoordinationNode().FileName))) {
            result = RestoreCoordinationNode(child, childDbPath, settings, oldEntries.contains(childDbPath));
        } else if (IsFileExists(child.Child(NFiles::CreateAsyncReplication().FileName))) {
            result = RestoreReplication(child, dbRestoreRoot, Join('/', dbPathRelativeToRestoreRoot, child.GetName()), settings, oldEntries.contains(childDbPath));
        } else if (IsFileExists(child.Child(NFiles::CreateExternalDataSource().FileName))) {
            result = RestoreExternalDataSource(child, childDbPath, settings, oldEntries.contains(childDbPath));
        } else if (IsFileExists(child.Child(NFiles::CreateExternalTable().FileName))) {
            DelayedRestoreManager.Add(ESchemeEntryType::ExternalTable, child, childDbPath, settings, oldEntries.contains(childDbPath));
        } else if (child.IsDirectory()) {
            result = RestoreFolder(child, dbRestoreRoot, Join('/', dbPathRelativeToRestoreRoot, child.GetName()), settings, oldEntries);
        }

        if (result.Defined() && !result->IsSuccess()) {
            return *result;
        }
    }

    const bool dbPathExists = oldEntries.contains(dbPath);
    if (!result.Defined() && !dbPathExists) {
        // This situation arises when all the children of the file system path are scheme objects with a delayed restoration.
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

    if (settings.DryRun_) {
        return CheckExistenceAndType(SchemeClient, dbPath, ESchemeEntryType::View);
    }

    TString query = ReadViewQuery(fsPath, Log.get());

    NYql::TIssues issues;
    const bool isDb = IsDatabase(SchemeClient, dbRestoreRoot);
    if (!RewriteCreateViewQuery(query, dbRestoreRoot, isDb, dbPath, issues)) {
        return Result<TRestoreResult>(fsPath.GetPath(), EStatus::BAD_REQUEST, issues.ToString());
    }

    auto result = QueryClient.RetryQuerySync([&](NQuery::TSession session) {
        return session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    });

    if (result.IsSuccess()) {
        LOG_D("Created " << dbPath.Quote());
        return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
    }

    if (result.GetStatus() == EStatus::SCHEME_ERROR) {
        LOG_I("Failed to create " << dbPath.Quote() << ". Will retry.");
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

    const auto request = ReadTopicCreationRequest(fsPath, Log.get());
    auto result = CreateTopic(TopicClient, dbPath, request);
    if (result.IsSuccess()) {
        LOG_D("Created " << dbPath.Quote());
        return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
    }

    LOG_E("Failed to create " << dbPath.Quote());
    return Result<TRestoreResult>(dbPath, std::move(result));
}

TRestoreResult TRestoreClient::CheckSecretExistence(const TString& secretName) {
    LOG_D("Check existence of the secret " << secretName.Quote());

    const auto tmpUser = CreateGuidAsString();
    const auto create = std::format("CREATE OBJECT `{}:{}` (TYPE SECRET_ACCESS);", secretName.c_str(), tmpUser.c_str());
    const auto drop = std::format("DROP OBJECT `{}:{}` (TYPE SECRET_ACCESS);", secretName.c_str(), tmpUser.c_str());

    auto result = QueryClient.RetryQuerySync([&](NQuery::TSession session) {
        return session.ExecuteQuery(create, NQuery::TTxControl::NoTx()).ExtractValueSync();
    });
    if (!result.IsSuccess()) {
        return Result<TRestoreResult>(EStatus::PRECONDITION_FAILED, TStringBuilder()
            << "Secret " << secretName.Quote() << " does not exist or you do not have access permissions");
    }

    result = QueryClient.RetryQuerySync([&](NQuery::TSession session) {
        return session.ExecuteQuery(drop, NQuery::TTxControl::NoTx()).ExtractValueSync();
    });
    if (!result.IsSuccess()) {
        return Result<TRestoreResult>(EStatus::INTERNAL_ERROR, TStringBuilder()
            << "Failed to drop temporary secret access " << secretName << ":" << tmpUser);
    }

    return result;
}

TRestoreResult TRestoreClient::RestoreReplication(
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
    LOG_I("Restore async replication " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    if (settings.DryRun_) {
        return CheckExistenceAndType(SchemeClient, dbPath, ESchemeEntryType::Replication);
    }

    auto query = ReadAsyncReplicationQuery(fsPath, Log.get());
    if (const auto secretName = GetSecretName(query)) {
        if (auto result = CheckSecretExistence(secretName); !result.IsSuccess()) {
            return Result<TRestoreResult>(fsPath.GetPath(), std::move(result));
        }
    }

    NYql::TIssues issues;
    if (!RewriteObjectRefs(query, dbRestoreRoot, issues)) {
        return Result<TRestoreResult>(fsPath.GetPath(), EStatus::BAD_REQUEST, issues.ToString());
    }
    if (!RewriteCreateQuery(query, "CREATE ASYNC REPLICATION `{}`", dbPath, issues)) {
        return Result<TRestoreResult>(fsPath.GetPath(), EStatus::BAD_REQUEST, issues.ToString());
    }

    auto result = QueryClient.RetryQuerySync([&](NQuery::TSession session) {
        return session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    });

    if (result.IsSuccess()) {
        LOG_D("Created " << dbPath.Quote());
        return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
    }

    LOG_E("Failed to create " << dbPath.Quote());
    return Result<TRestoreResult>(dbPath, std::move(result));
}

TRestoreResult TRestoreClient::RestoreRateLimiter(
    const TFsPath& fsPath,
    const TString& coordinationNodePath,
    const TString& rateLimiterPath)
{
    LOG_D("Process " << fsPath.GetPath().Quote());

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    const auto request = ReadRateLimiterCreationRequest(fsPath, Log.get());
    auto result = CreateRateLimiter(RateLimiterClient, coordinationNodePath, rateLimiterPath, request);
    if (result.IsSuccess()) {
        LOG_D("Created rate limiter: " << rateLimiterPath.Quote()
            << " dependent on the coordination node: " << coordinationNodePath.Quote()
        );
        return Result<TRestoreResult>();
    }

    LOG_E("Failed to create rate limiter: " << rateLimiterPath.Quote()
        << " dependent on the coordination node: " << coordinationNodePath.Quote()
    );
    return Result<TRestoreResult>(Join("/", coordinationNodePath, rateLimiterPath), std::move(result));
}

TRestoreResult TRestoreClient::RestoreDependentResources(const TFsPath& fsPath, const TString& dbPath) {
    LOG_I("Restore coordination node's resources " << fsPath.GetPath().Quote()
        << " to " << dbPath.Quote()
    );

    TVector<TFsPath> children;
    fsPath.List(children);
    TDeque<TFsPath> pathQueue(children.begin(), children.end());
    while (!pathQueue.empty()) {
        const auto path = pathQueue.front();
        pathQueue.pop_front();

        if (path.IsDirectory()) {
            if (IsFileExists(path.Child(NFiles::CreateRateLimiter().FileName))) {
                const auto result = RestoreRateLimiter(path, dbPath, path.RelativeTo(fsPath).GetPath());
                if (!result.IsSuccess()) {
                    return result;
                }
            }

            children.clear();
            path.List(children);
            pathQueue.insert(pathQueue.end(), children.begin(), children.end());
        }

    }

    return Result<TRestoreResult>();
}

TRestoreResult TRestoreClient::RestoreCoordinationNode(
    const TFsPath& fsPath,
    const TString& dbPath,
    const TRestoreSettings& settings,
    bool isAlreadyExisting)
{
    LOG_D("Process " << fsPath.GetPath().Quote());

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    LOG_I("Restore coordination node " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    if (settings.DryRun_) {
        return CheckExistenceAndType(SchemeClient, dbPath, ESchemeEntryType::CoordinationNode);
    }

    const auto request = ReadCoordinationNodeCreationRequest(fsPath, Log.get());
    auto result = CreateCoordinationNode(CoordinationNodeClient, dbPath, request);
    if (result.IsSuccess()) {
        if (auto result = RestoreDependentResources(fsPath, dbPath); !result.IsSuccess()) {
            LOG_E("Failed to create coordination node's resources " << dbPath.Quote());
            return Result<TRestoreResult>(dbPath, std::move(result));
        }

        LOG_D("Created " << dbPath.Quote());
        return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
    }

    LOG_E("Failed to create " << dbPath.Quote());
    return Result<TRestoreResult>(dbPath, std::move(result));
}

TRestoreResult TRestoreClient::RestoreExternalDataSource(
    const TFsPath& fsPath,
    const TString& dbPath,
    const TRestoreSettings& settings,
    bool isAlreadyExisting)
{
    LOG_D("Process " << fsPath.GetPath().Quote());

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    LOG_I("Restore external data source " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    if (settings.DryRun_) {
        return CheckExistenceAndType(SchemeClient, dbPath, ESchemeEntryType::ExternalDataSource);
    }

    TString query = ReadExternalDataSourceQuery(fsPath, Log.get());
    if (const auto secretName = GetSecretName(query)) {
        if (auto result = CheckSecretExistence(secretName); !result.IsSuccess()) {
            return Result<TRestoreResult>(fsPath.GetPath(), std::move(result));
        }
    }

    NYql::TIssues issues;
    if (!RewriteCreateQuery(query, "CREATE EXTERNAL DATA SOURCE IF NOT EXISTS `{}`", dbPath, issues)) {
        return Result<TRestoreResult>(fsPath.GetPath(), EStatus::BAD_REQUEST, issues.ToString());
    }

    auto result = QueryClient.RetryQuerySync([&](NQuery::TSession session) {
        return session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    });

    if (result.IsSuccess()) {
        LOG_D("Created " << dbPath.Quote());
        return RestorePermissions(fsPath, dbPath, settings, isAlreadyExisting);
    }

    LOG_E("Failed to create " << dbPath.Quote());
    return Result<TRestoreResult>(dbPath, std::move(result));
}

TRestoreResult TRestoreClient::RestoreExternalTable(
    const TFsPath& fsPath,
    const TString& dbPath,
    const TRestoreSettings& settings,
    bool isAlreadyExisting)
{
    LOG_D("Process " << fsPath.GetPath().Quote());

    if (auto error = ErrorOnIncomplete(fsPath)) {
        return *error;
    }

    LOG_I("Restore external table " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    if (settings.DryRun_) {
        return CheckExistenceAndType(SchemeClient, dbPath, ESchemeEntryType::ExternalTable);
    }

    TString query = ReadExternalTableQuery(fsPath, Log.get());

    NYql::TIssues issues;
    if (!RewriteCreateQuery(query, "CREATE EXTERNAL TABLE IF NOT EXISTS `{}`", dbPath, issues)) {
        return Result<TRestoreResult>(fsPath.GetPath(), EStatus::BAD_REQUEST, issues.ToString());
    }

    auto result = QueryClient.RetryQuerySync([&](NQuery::TSession session) {
        return session.ExecuteQuery(query, NQuery::TTxControl::NoTx()).ExtractValueSync();
    });

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
        const ui32 partitionCount = scheme.partition_at_keys().split_points().size() + 1;
        auto result = RestoreData(fsPath, dbPath, settings, withoutIndexesDesc, partitionCount);
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
        ui32 partitionCount,
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
            writer.Reset(CreateImportDataWriter(dbPath, desc, partitionCount, ImportClient, TableClient, accumulators, settings, Log));
            break;
        }
    }

    return writer;
}

TRestoreResult TRestoreClient::CreateDataAccumulators(
        TVector<THolder<NPrivate::IDataAccumulator>>& outAccumulators,
        const TString& dbPath,
        const TRestoreSettings& settings,
        const TTableDescription& desc,
        ui32 dataFilesCount)
{
    size_t accumulatorsCount = settings.MaxInFlight_;
    if (!accumulatorsCount) {
        accumulatorsCount = Min<size_t>(dataFilesCount, NSystemInfo::CachedNumberOfCpus());
    }

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
        const TTableDescription& desc,
        ui32 partitionCount)
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

    THolder<NPrivate::IDataWriter> writer = CreateDataWriter(dbPath, settings, desc, partitionCount, accumulators);

    TVector<TFuture<TRestoreResult>> accumulatorResults(Reserve(accumulators.size()));
    TThreadPool accumulatorWorkers(TThreadPool::TParams().SetBlocking(true));
    accumulatorWorkers.Start(accumulators.size(), accumulators.size());

    const ui32 dataFilesPerAccumulator = dataFilesCount / accumulators.size();
    const ui32 dataFilesPerAccumulatorRemainder = dataFilesCount % accumulators.size();

    for (ui32 i = 0; i < accumulators.size(); ++i) {
        auto* accumulator = accumulators[i].Get();
        auto promise = NewPromise<TRestoreResult>();
        accumulatorResults.emplace_back(promise);

        ui32 idStart = dataFilesPerAccumulator * i + std::min(i, dataFilesPerAccumulatorRemainder);
        ui32 idEnd = dataFilesPerAccumulator * (i + 1) + std::min(i + 1, dataFilesPerAccumulatorRemainder);

        auto func = [&, idStart, idEnd, accumulator, result = std::move(promise)]() mutable {
            try {
                for (size_t id = idStart; id < idEnd; ++id) {
                    const TFsPath& dataFile = dataFiles[id];

                    LOG_D("Read data from " << dataFile.GetPath().Quote());

                    TFileInput input(dataFile, settings.FileBufferSize_);
                    TString line;
                    ui64 lineNo = 0;

                    while (input.ReadLine(line)) {
                        auto l = NPrivate::TLine(std::move(line), dataFile.GetPath(), ++lineNo);

                        for (auto status = accumulator->Check(l); status != NPrivate::IDataAccumulator::OK; status = accumulator->Check(l)) {
                            if (status == NPrivate::IDataAccumulator::ERROR) {
                                return result.SetValue(Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR,
                                    TStringBuilder() << "Invalid data: " << l.GetLocation()));
                            }

                            if (!accumulator->Ready(true)) {
                                LOG_E("Error reading data from " << dataFile.GetPath().Quote());
                                return result.SetValue(Result<TRestoreResult>(dbPath, EStatus::INTERNAL_ERROR, "Data is not ready"));
                            }

                            if (!writer->Push(accumulator->GetData(true))) {
                                LOG_E("Error writing data to " << dbPath.Quote() << ", file: " << dataFile.GetPath().Quote());
                                return result.SetValue(Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #1"));
                            }
                        }

                        accumulator->Feed(std::move(l));
                        if (accumulator->Ready()) {
                            if (!writer->Push(accumulator->GetData())) {
                                LOG_E("Error writing data to " << dbPath.Quote() << ", file: " << dataFile.GetPath().Quote());
                                return result.SetValue(Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #2"));
                            }
                        }
                    }
                }

                while (accumulator->Ready(true)) {
                    if (!writer->Push(accumulator->GetData(true))) {
                        return result.SetValue(Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Cannot write data #3"));
                    }
                }

                result.SetValue(Result<TRestoreResult>());
            } catch (...) {
                result.SetException(std::current_exception());
            }
        };

        if (!accumulatorWorkers.AddFunc(std::move(func))) {
            return Result<TRestoreResult>(dbPath, EStatus::GENERIC_ERROR, "Can't start restoring data: queue is full or shutting down");
        }
    }

    accumulatorWorkers.Stop();
    if (auto res = CombineResults(accumulatorResults); !res.IsSuccess()) {
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
            writer = CreateDataWriter(dbPath, settings, desc, partitionCount, accumulators);
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

        auto forgetStatus = RetryFunction([&]() {
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

TRestoreResult TRestoreClient::RestorePermissionsImpl(
    TSchemeClient& client,
    const TFsPath& fsPath,
    const TString& dbPath)
{
    if (!fsPath.Child(NFiles::Permissions().FileName).Exists()) {
        return Result<TRestoreResult>();
    }

    LOG_D("Restore ACL " << fsPath.GetPath().Quote() << " to " << dbPath.Quote());

    auto permissions = ReadPermissions(fsPath, Log.get());
    auto result = ModifyPermissions(client, dbPath, TModifyPermissionsSettings(permissions));

    if (result.GetStatus() == EStatus::UNAUTHORIZED) {
        LOG_W("Not enough rights to restore permissions on " << dbPath.Quote() << ", skipping");
        return Result<TRestoreResult>();
    }

    return result;
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

    return RestorePermissionsImpl(SchemeClient, fsPath, dbPath);
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
