#include "backup.h"
#include "db_iterator.h"
#include "util.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/cms/cms.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_view.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/api/protos/draft/ydb_view.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_view.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/rate_limiter/rate_limiter.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>
#include <yql/essentials/sql/v1/format/sql_format.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/regex/pcre/regexp.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/folder/pathsplit.h>
#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/mem.h>
#include <util/stream/null.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/fs.h>

#include <google/protobuf/text_format.h>

#include <format>
#include <ranges>

namespace NYdb::NBackup {

static constexpr size_t IO_BUFFER_SIZE = 2 << 20; // 2 MiB
static constexpr i64 FILE_SPLIT_THRESHOLD = 128 << 20; // 128 MiB
static constexpr i64 READ_TABLE_RETRIES = 100;


////////////////////////////////////////////////////////////////////////////////
//                               Util
////////////////////////////////////////////////////////////////////////////////

void TYdbErrorException::LogToStderr() const {
    Cerr << Status;
}

static void VerifyStatus(TStatus status, TString explain = "") {
    if (status.IsSuccess()) {
        if (status.GetIssues()) {
            LOG_D(status);
        }
    } else {
        if (explain) {
            LOG_E(explain << ": " << status.GetIssues().ToOneLineString());
        }
        throw TYdbErrorException(status) << explain;
    }
}

static TString JoinDatabasePath(const TString& basePath, const TString& path) {
    if (basePath.empty()) {
        return path;
    } else if (path == "/") {
        return basePath;
    } else if (basePath == "/") {
        return path;
    } else {
        TPathSplitUnix prefixPathSplit(basePath);
        prefixPathSplit.AppendComponent(path);

        return prefixPathSplit.Reconstruct();
    }
}

static TString CreateDataFileName(ui32 i) {
    return Sprintf("data_%02d.csv", i);
}

static TString CreateTemporalBackupName() {
    return "backup_" + TInstant::Now().FormatLocalTime("%Y%m%dT%H%M%S"); // YYYYMMDDThhmmss
}

////////////////////////////////////////////////////////////////////////////////
//                               Backup
////////////////////////////////////////////////////////////////////////////////

#define CASE_PRINT_PRIMITIVE_TYPE(out, type)        \
case EPrimitiveType::type:                          \
    out << parser.Get##type();                      \
    break;

#define CASE_PRINT_PRIMITIVE_STRING_TYPE(out, type) \
case EPrimitiveType::type: {                        \
        auto str = TString{parser.Get##type()};           \
        CGIEscape(str);                             \
        out << '"' << str << '"';                   \
    }                                               \
    break;

void PrintPrimitive(IOutputStream& out, const TValueParser& parser) {
    switch(parser.GetPrimitiveType()) {
        CASE_PRINT_PRIMITIVE_TYPE(out, Bool);
        case EPrimitiveType::Int8:
            out << (i32)parser.GetInt8();
            break;
        case EPrimitiveType::Uint8:
            out << (ui32)parser.GetUint8();
            break;
        CASE_PRINT_PRIMITIVE_TYPE(out, Int16);
        CASE_PRINT_PRIMITIVE_TYPE(out, Uint16);
        CASE_PRINT_PRIMITIVE_TYPE(out, Int32);
        CASE_PRINT_PRIMITIVE_TYPE(out, Uint32);
        CASE_PRINT_PRIMITIVE_TYPE(out, Int64);
        CASE_PRINT_PRIMITIVE_TYPE(out, Uint64);
        CASE_PRINT_PRIMITIVE_TYPE(out, Float);
        CASE_PRINT_PRIMITIVE_TYPE(out, Double);
        CASE_PRINT_PRIMITIVE_TYPE(out, DyNumber);
        CASE_PRINT_PRIMITIVE_TYPE(out, Date);
        CASE_PRINT_PRIMITIVE_TYPE(out, Datetime);
        CASE_PRINT_PRIMITIVE_TYPE(out, Timestamp);
        CASE_PRINT_PRIMITIVE_TYPE(out, Interval);
        CASE_PRINT_PRIMITIVE_TYPE(out, Date32);
        CASE_PRINT_PRIMITIVE_TYPE(out, Datetime64);
        CASE_PRINT_PRIMITIVE_TYPE(out, Timestamp64);
        CASE_PRINT_PRIMITIVE_TYPE(out, Interval64);
        CASE_PRINT_PRIMITIVE_STRING_TYPE(out, TzDate);
        CASE_PRINT_PRIMITIVE_STRING_TYPE(out, TzDatetime);
        CASE_PRINT_PRIMITIVE_STRING_TYPE(out, TzTimestamp);
        CASE_PRINT_PRIMITIVE_STRING_TYPE(out, String);
        CASE_PRINT_PRIMITIVE_STRING_TYPE(out, Utf8);
        CASE_PRINT_PRIMITIVE_STRING_TYPE(out, Yson);
        CASE_PRINT_PRIMITIVE_STRING_TYPE(out, Json);
        CASE_PRINT_PRIMITIVE_STRING_TYPE(out, JsonDocument);
        default:
            Y_ABORT("Unsupported type");
    }
}
#undef CASE_PRINT_PRIMITIVE_STRING_TYPE
#undef CASE_PRINT_PRIMITIVE_TYPE

void PrintValue(IOutputStream& out, TValueParser& parser) {
    switch (parser.GetKind()) {
        case TTypeParser::ETypeKind::Primitive:
            PrintPrimitive(out, parser);
            break;

        case TTypeParser::ETypeKind::Decimal: {
            auto decimal = parser.GetDecimal();
            out << decimal.ToString();
            break;
        }

        case TTypeParser::ETypeKind::Optional:
            parser.OpenOptional();
            if (parser.IsNull()) {
                out << "null";
            } else {
                PrintValue(out, parser);
            }
            parser.CloseOptional();
            break;

        case TTypeParser::ETypeKind::Void:
            out << "Void"sv;
            break;

        case TTypeParser::ETypeKind::List:
            /* fallthrough */
        case TTypeParser::ETypeKind::Tuple:
            /* fallthrough */
        case TTypeParser::ETypeKind::Struct:
            /* fallthrough */
        case TTypeParser::ETypeKind::Dict:
            /* fallthrough */
        case TTypeParser::ETypeKind::Variant:
            Y_ENSURE(false, TStringBuilder() << "This typeKind is not supported for backup now,"
                    " kind: " << parser.GetKind());
            break;

        default:
            ThrowFatalError(TStringBuilder()
                << "Unexpected type kind: " << parser.GetKind());
    }
}

TMaybe<TValue> ProcessResultSet(TStringStream& ss,
        TResultSetParser& resultSetParser, TFile* dataFile, const NTable::TTableDescription* desc) {
    TMaybe<TValue> lastReadPK;

    TStackVec<TValueParser*, 32> colParsers;
    if (desc) {
        Y_ENSURE(resultSetParser.ColumnsCount() == desc->GetColumns().size(), "resultSet got from ReadTable has "
                "number of columns differing from DescribeTable");
        for (const auto& col : desc->GetColumns()) {
            colParsers.push_back(&resultSetParser.ColumnParser(col.Name));
        }
    } else {
        for (ui32 i = 0; i < resultSetParser.ColumnsCount(); ++i) {
            colParsers.push_back(&resultSetParser.ColumnParser(i));
        }
    }
    while (resultSetParser.TryNextRow()) {
        bool needsComma = false;
        for (auto* parser : colParsers) {
            if (needsComma) {
                ss << ",";
            } else {
                needsComma = true;
            }
            PrintValue(ss, *parser);
        }
        ss << "\n";
        if (dataFile && ss.Size() > IO_BUFFER_SIZE / 2) {
            dataFile->Write(ss.Data(), ss.Size());
            ss.Clear();
        }
        if (desc) {
            TValueBuilder value;
            value.BeginTuple();
            for (const auto& col : desc->GetPrimaryKeyColumns()) {
                value.AddElement(resultSetParser.GetValue(col));
            }
            value.EndTuple();
            lastReadPK = value.Build();
        }
    }
    return lastReadPK;
}

static void Flush(TFile& tmpFile, TStringStream& ss, TMaybe<TValue>& lastWrittenPK, const TMaybe<TValue>& lastReadPK) {
    tmpFile.Write(ss.Data(), ss.Size());
    ss.Clear();

    if (lastReadPK) {
        lastWrittenPK = *lastReadPK;
    }
}

static void CloseAndRename(TFile& tmpFile, const TFsPath& fileName) {
    tmpFile.Close();

    LOG_D("Write data into " << fileName.GetPath().Quote());
    TFsPath(tmpFile.GetName()).RenameTo(fileName);
}

TMaybe<TValue> TryReadTable(TDriver driver, const NTable::TTableDescription& desc, const TString& fullTablePath,
        const TFsPath& folderPath, TMaybe<TValue> lastWrittenPK, ui32 *fileCounter, bool ordered)
{
    TMaybe<NTable::TTablePartIterator> iter;
    auto readTableJob = [fullTablePath, &lastWrittenPK, &iter, &ordered](NTable::TSession session) -> TStatus {
        NTable::TReadTableSettings settings;
        if (lastWrittenPK) {
            settings.From(NTable::TKeyBound::Exclusive(*lastWrittenPK));
        }
        if (ordered) {
            settings.Ordered();
        }
        auto result = session.ReadTable(fullTablePath, settings).ExtractValueSync();
        if (result.IsSuccess()) {
            iter = result;
        }
        VerifyStatus(result, TStringBuilder() << "Read table " << fullTablePath.Quote() << " failed");
        return result;
    };

    NTable::TTableClient client(driver);
    TStatus status = client.RetryOperationSync(readTableJob, NTable::TRetryOperationSettings().MaxRetries(1));
    VerifyStatus(status);

    {
        auto resultSetStreamPart = iter->ReadNext().ExtractValueSync();
        if (!resultSetStreamPart.IsSuccess() && resultSetStreamPart.EOS()) {
            // Table is empty, so create empty data file
            TFile dataFile(folderPath.Child(CreateDataFileName((*fileCounter)++)), CreateAlways | WrOnly);
            return {};
        }
        VerifyStatus(resultSetStreamPart, TStringBuilder() << "Read next part of " << fullTablePath.Quote() << " failed");
        TResultSet resultSetCurrent = resultSetStreamPart.ExtractPart();

        auto tmpFile = TFile(folderPath.Child(NDump::NFiles::IncompleteData().FileName), CreateAlways | WrOnly);
        TStringStream ss;
        ss.Reserve(IO_BUFFER_SIZE);

        TMaybe<TValue> lastReadPK;

        while (true) {
            NTable::TAsyncSimpleStreamPart<TResultSet> nextResult;
            if (resultSetCurrent.Truncated()) {
                nextResult = iter->ReadNext();
            }
            auto resultSetParser = TResultSetParser(resultSetCurrent);
            lastReadPK = ProcessResultSet(ss, resultSetParser, &tmpFile, &desc);

            // Next
            if (resultSetCurrent.Truncated()) {
                auto resultSetStreamPart = nextResult.ExtractValueSync();
                if (!resultSetStreamPart.IsSuccess()) {
                    if (resultSetStreamPart.EOS()) {
                        break;
                    } else {
                        LOG_D("Stream was closed unexpectedly: " << resultSetStreamPart.GetIssues().ToOneLineString());
                        if (ss.Data()) {
                            Flush(tmpFile, ss, lastWrittenPK, lastReadPK);
                        }
                        CloseAndRename(tmpFile, folderPath.Child(CreateDataFileName((*fileCounter)++)));
                        return lastWrittenPK;
                    }
                }
                resultSetCurrent = resultSetStreamPart.ExtractPart();
            } else {
                break;
            }
            if (ss.Size() > IO_BUFFER_SIZE) {
                Flush(tmpFile, ss, lastWrittenPK, lastReadPK);
            }
            if (tmpFile.GetLength() > FILE_SPLIT_THRESHOLD) {
                CloseAndRename(tmpFile, folderPath.Child(CreateDataFileName((*fileCounter)++)));
                tmpFile = TFile(folderPath.Child(NDump::NFiles::IncompleteData().FileName), CreateAlways | WrOnly);
            }
        }
        Flush(tmpFile, ss, lastWrittenPK, lastReadPK);
        CloseAndRename(tmpFile, folderPath.Child(CreateDataFileName((*fileCounter)++)));
    }

    return {};
}

void ReadTable(TDriver driver, const NTable::TTableDescription& desc, const TString& fullTablePath,
        const TFsPath& folderPath, bool ordered) {
    LOG_D("Read table " << fullTablePath.Quote());

    TMaybe<TValue> lastWrittenPK;

    i64 retries = READ_TABLE_RETRIES;
    ui32 fileCounter = 0;
    do {
        lastWrittenPK = TryReadTable(driver, desc, fullTablePath, folderPath, lastWrittenPK, &fileCounter, ordered);
        if (lastWrittenPK && retries) {
            LOG_D("Retry read table from key: " << TString{FormatValueYson(*lastWrittenPK)}.Quote());
        }
    } while (lastWrittenPK && retries--);

    Y_ENSURE(!lastWrittenPK, "For table " << fullTablePath.Quote() << " ReadTable hasn't finished successfully after "
            << READ_TABLE_RETRIES << " retries");
}

NTable::TTableDescription DescribeTable(TDriver driver, const TString& fullTablePath) {
    LOG_D("Describe table " << fullTablePath.Quote());

    TMaybe<NTable::TTableDescription> desc;

    NTable::TTableClient client(driver);
    TStatus status = client.RetryOperationSync([fullTablePath, &desc](NTable::TSession session) {
        auto settings = NTable::TDescribeTableSettings().WithKeyShardBoundary(true).WithSetVal(true);
        auto result = session.DescribeTable(fullTablePath, settings).GetValueSync();

        VerifyStatus(result);
        desc = result.GetTableDescription();
        return result;
    });
    VerifyStatus(status);

    return *desc;
}

Ydb::Table::CreateTableRequest ProtoFromTableDescription(const NTable::TTableDescription& desc, bool preservePoolKinds) {
    Ydb::Table::CreateTableRequest proto;
    desc.SerializeTo(proto);

    if (preservePoolKinds) {
        return proto;
    }

    if (proto.has_profile()) {
        auto& profile = *proto.mutable_profile();

        if (profile.has_storage_policy()) {
            auto& policy = *profile.mutable_storage_policy();

            policy.clear_syslog();
            policy.clear_log();
            policy.clear_data();
            policy.clear_external();

            for (auto& family : *policy.mutable_column_families()) {
                family.clear_data();
                family.clear_external();
            }
        }
    }

    if (proto.has_storage_settings()) {
        auto& settings = *proto.mutable_storage_settings();

        settings.clear_tablet_commit_log0();
        settings.clear_tablet_commit_log1();
        settings.clear_external();
    }

    for (auto& family : *proto.mutable_column_families()) {
        family.clear_data();
    }

    return proto;
}

NScheme::TSchemeEntry DescribePath(TDriver driver, const TString& fullPath) {
    NScheme::TSchemeClient client(driver);

    auto status = NDump::DescribePath(client, fullPath);
    VerifyStatus(status);

    return status.GetEntry();
}

TAsyncStatus CopyTableAsyncStart(TDriver driver, const TString& src, const TString& dst) {
    LOG_I("Copy table " << src.Quote() << " to " << dst.Quote());

    NTable::TTableClient client(driver);
    return client.RetryOperation([src, dst](NTable::TSession session) {
        auto result = session.CopyTable(src, dst);
        return result;
    });
}

void CopyTableAsyncFinish(const TAsyncStatus& status, const TString& src) {
    VerifyStatus(status.GetValueSync(), TStringBuilder() << "Copy table " << src.Quote() << " failed");
}

void CopyTables(TDriver driver, const TVector<NTable::TCopyItem>& tablesToCopy) {
    LOG_I("Copy tables: " << JoinSeq(", ", tablesToCopy));

    NTable::TTableClient client(driver);
    TStatus status = client.RetryOperationSync([&tablesToCopy](NTable::TSession session) {
        auto result = session.CopyTables(tablesToCopy).GetValueSync();
        return result;
    });

    VerifyStatus(status, "Copy tables failed");
}

void DropTable(TDriver driver, const TString& path) {
    LOG_D("Drop table " << path.Quote());

    NTable::TTableClient client(driver);
    TStatus status = client.RetryOperationSync([path](NTable::TSession session) {
        auto result = session.DropTable(path).GetValueSync();
        return result;
    });

    VerifyStatus(status, TStringBuilder() << "Drop table " << path.Quote() << " failed");
}

TFsPath CreateDirectory(const TFsPath& folderPath, const TString& name) {
    TFsPath childFolderPath = folderPath.Child(name);
    LOG_D("Process " << childFolderPath.GetPath().Quote());
    childFolderPath.MkDir();
    return childFolderPath;
}

void WriteProtoToFile(const google::protobuf::Message& proto, const TFsPath& folderPath, const NDump::NFiles::TFileInfo& fileInfo) {
    TString protoStr;
    google::protobuf::TextFormat::PrintToString(proto, &protoStr);
    LOG_D("Write " << fileInfo.LogObjectType << " into " << folderPath.Child(fileInfo.FileName).GetPath().Quote());
    TFile outFile(folderPath.Child(fileInfo.FileName), CreateAlways | WrOnly);
    outFile.Write(protoStr.data(), protoStr.size());
}

void WriteCreationQueryToFile(const TString& creationQuery, const TFsPath& folderPath, const NDump::NFiles::TFileInfo& fileInfo) {
    LOG_D("Write " << fileInfo.LogObjectType << " into " << folderPath.Child(fileInfo.FileName).GetPath().Quote());
    TFile outFile(folderPath.Child(fileInfo.FileName), CreateAlways | WrOnly);
    outFile.Write(creationQuery.data(), creationQuery.size());
}

void BackupPermissions(TDriver driver, const TString& dbPath, const TFsPath& folderPath) {
    auto entry = DescribePath(driver, dbPath);
    Ydb::Scheme::ModifyPermissionsRequest proto;
    entry.SerializeTo(proto);
    WriteProtoToFile(proto, folderPath, NDump::NFiles::Permissions());
}

void BackupPermissions(TDriver driver, const TString& dbPrefix, const TString& path, const TFsPath& folderPath) {
    BackupPermissions(driver, JoinDatabasePath(dbPrefix, path), folderPath);
}

Ydb::Table::ChangefeedDescription ProtoFromChangefeedDesc(const NTable::TChangefeedDescription& changefeedDesc) {
    Ydb::Table::ChangefeedDescription protoChangeFeedDesc;
    changefeedDesc.SerializeTo(protoChangeFeedDesc);
    return protoChangeFeedDesc;
}

NTopic::TTopicDescription DescribeTopic(TDriver driver, const TString& path) {
    NTopic::TTopicClient client(driver);
    const auto result = NConsoleClient::RetryFunction([&]() {
        return client.DescribeTopic(path).ExtractValueSync();
    });
    VerifyStatus(result, "describe topic");
    return result.GetTopicDescription();
}

void BackupChangefeeds(TDriver driver, const TString& tablePath, const TFsPath& folderPath) {
    auto desc = DescribeTable(driver, tablePath);

    for (const auto& changefeedDesc : desc.GetChangefeedDescriptions()) {
        TFsPath changefeedDirPath = CreateDirectory(folderPath, TString{changefeedDesc.GetName()});

        auto protoChangeFeedDesc = ProtoFromChangefeedDesc(changefeedDesc);
        const auto topicDescription = DescribeTopic(driver, JoinDatabasePath(tablePath, TString{changefeedDesc.GetName()}));
        auto protoTopicDescription = NYdb::TProtoAccessor::GetProto(topicDescription);
        // Unnecessary fields
        protoTopicDescription.clear_self();
        protoTopicDescription.clear_topic_stats();

        WriteProtoToFile(protoChangeFeedDesc, changefeedDirPath, NDump::NFiles::Changefeed());
        WriteProtoToFile(protoTopicDescription, changefeedDirPath, NDump::NFiles::TopicDescription());
    }
}

void BackupTable(TDriver driver, const TString& dbPrefix, const TString& backupPrefix, const TString& path,
        const TFsPath& folderPath, bool schemaOnly, bool preservePoolKinds, bool ordered) {
    Y_ENSURE(!path.empty());
    Y_ENSURE(path.back() != '/', path.Quote() << " path contains / in the end");

    const auto fullPath = JoinDatabasePath(schemaOnly ? dbPrefix : backupPrefix, path);

    LOG_I("Backup table " << fullPath.Quote() << " to " << folderPath.GetPath().Quote());

    auto desc = DescribeTable(driver, fullPath);
    auto proto = ProtoFromTableDescription(desc, preservePoolKinds);
    WriteProtoToFile(proto, folderPath, NDump::NFiles::TableScheme());

    BackupChangefeeds(driver, JoinDatabasePath(dbPrefix, path), folderPath);
    BackupPermissions(driver, dbPrefix, path, folderPath);

    if (!schemaOnly) {
        ReadTable(driver, desc, fullPath, folderPath, ordered);
    }
}

namespace {

NView::TViewDescription DescribeView(TDriver driver, const TString& path) {
    NView::TViewClient client(driver);
    auto status = NConsoleClient::RetryFunction([&]() {
        return client.DescribeView(path).ExtractValueSync();
    });
    VerifyStatus(status, "describe view");
    return status.GetViewDescription();
}

}

/*!
The BackupView function retrieves the view's description from the database,
constructs a corresponding CREATE VIEW statement,
and writes it to the backup folder designated for this view.

\param dbBackupRoot the root of the backup in the database
\param dbPathRelativeToBackupRoot the path to the view in the database relative to the backup root
\param fsBackupFolder the path on the file system to write the file with the CREATE VIEW statement to
\param issues the accumulated backup issues container
*/
void BackupView(TDriver driver, const TString& dbBackupRoot, const TString& dbPathRelativeToBackupRoot,
    const TFsPath& fsBackupFolder, NYql::TIssues& issues
) {
    Y_ENSURE(!dbPathRelativeToBackupRoot.empty());
    const auto dbPath = JoinDatabasePath(dbBackupRoot, dbPathRelativeToBackupRoot);

    LOG_I("Backup view " << dbPath.Quote() << " to " << fsBackupFolder.GetPath().Quote());

    const auto viewDescription = DescribeView(driver, dbPath);

    const auto creationQuery = NDump::BuildCreateViewQuery(
        TFsPath(dbPathRelativeToBackupRoot).GetName(),
        dbPath,
        TString(viewDescription.GetQueryText()),
        dbBackupRoot,
        issues
    );
    Y_ENSURE(creationQuery, issues.ToString());

    WriteCreationQueryToFile(creationQuery, fsBackupFolder, NDump::NFiles::CreateView());
    BackupPermissions(driver, dbPath, fsBackupFolder);
}

void BackupTopic(TDriver driver, const TString& dbPath, const TFsPath& fsBackupFolder) {
    Y_ENSURE(!dbPath.empty());
    LOG_I("Backup topic " << dbPath.Quote() << " to " << fsBackupFolder.GetPath().Quote());

    const auto topicDescription = DescribeTopic(driver, dbPath);

    Ydb::Topic::CreateTopicRequest creationRequest;
    topicDescription.SerializeTo(creationRequest);
    creationRequest.clear_attributes();

    WriteProtoToFile(creationRequest, fsBackupFolder, NDump::NFiles::CreateTopic());
    BackupPermissions(driver, dbPath, fsBackupFolder);
}

namespace {

NCoordination::TNodeDescription DescribeCoordinationNode(TDriver driver, const TString& path) {
    NCoordination::TClient client(driver);
    auto status = NConsoleClient::RetryFunction([&]() {
        return client.DescribeNode(path).ExtractValueSync();
    });
    VerifyStatus(status, "describe coordination node");
    return status.ExtractResult();
}

std::vector<std::string> ListRateLimiters(NRateLimiter::TRateLimiterClient& client, const std::string& coordinationNodePath) {
    const auto settings = NRateLimiter::TListResourcesSettings().Recursive(true);
    const std::string AllRootResourcesTag = "";
    auto status = NConsoleClient::RetryFunction([&]() {
        return client.ListResources(coordinationNodePath, AllRootResourcesTag, settings).ExtractValueSync();
    });
    VerifyStatus(status, "list rate limiters");
    return status.GetResourcePaths();
}

NRateLimiter::TDescribeResourceResult DescribeRateLimiter(
    NRateLimiter::TRateLimiterClient& client, const std::string& coordinationNodePath, const std::string& rateLimiterPath)
{
    auto status = NConsoleClient::RetryFunction([&]() {
        return client.DescribeResource(coordinationNodePath, rateLimiterPath).ExtractValueSync();
    });
    VerifyStatus(status, "describe rate limiter");
    return status;
}

void BackupDependentResources(TDriver driver, const std::string& coordinationNodePath, const TFsPath& fsBackupFolder) {
    NRateLimiter::TRateLimiterClient client(driver);
    const auto rateLimiters = ListRateLimiters(client, coordinationNodePath);

    for (const auto& rateLimiterPath : rateLimiters) {
        const auto desc = DescribeRateLimiter(client, coordinationNodePath, rateLimiterPath);
        Ydb::RateLimiter::CreateResourceRequest request;
        desc.GetHierarchicalDrrProps().SerializeTo(*request.mutable_resource()->mutable_hierarchical_drr());
        if (const auto& meteringConfig = desc.GetMeteringConfig()) {
            meteringConfig->SerializeTo(*request.mutable_resource()->mutable_metering_config());
        }

        TFsPath childFolderPath = fsBackupFolder.Child(TString{rateLimiterPath});
        childFolderPath.MkDirs();
        WriteProtoToFile(request, childFolderPath, NDump::NFiles::CreateRateLimiter());
    }
}

}

void BackupCoordinationNode(TDriver driver, const TString& dbPath, const TFsPath& fsBackupFolder) {
    Y_ENSURE(!dbPath.empty());
    LOG_I("Backup coordination node " << dbPath.Quote() << " to " << fsBackupFolder.GetPath().Quote());

    const auto nodeDescription = DescribeCoordinationNode(driver, dbPath);

    Ydb::Coordination::CreateNodeRequest creationRequest;
    nodeDescription.SerializeTo(creationRequest);

    WriteProtoToFile(creationRequest, fsBackupFolder, NDump::NFiles::CreateCoordinationNode());
    BackupDependentResources(driver, dbPath, fsBackupFolder);
    BackupPermissions(driver, dbPath, fsBackupFolder);
}

namespace {

NReplication::TReplicationDescription DescribeReplication(TDriver driver, const TString& path) {
    NReplication::TReplicationClient client(driver);
    auto status = NConsoleClient::RetryFunction([&]() {
        return client.DescribeReplication(path).ExtractValueSync();
    });
    VerifyStatus(status, "describe async replication");
    return status.GetReplicationDescription();
}

TString BuildConnectionString(const NReplication::TConnectionParams& params) {
    return TStringBuilder()
        << (params.GetEnableSsl() ? "grpcs://" : "grpc://")
        << params.GetDiscoveryEndpoint()
        << "/?database=" << params.GetDatabase();
}

inline TString BuildTarget(const char* src, const char* dst) {
    return TStringBuilder() << "  `" << src << "` AS `" << dst << "`";
}

inline TString Quote(const char* value) {
    return TStringBuilder() << "'" << value << "'";
}

template <typename StringType>
inline TString Quote(const StringType& value) {
    return Quote(value.c_str());
}

inline TString BuildOption(const char* key, const TString& value) {
    return TStringBuilder() << "  " << key << " = " << value << "";
}

inline TString Interval(const TDuration& value) {
    return TStringBuilder() << "Interval('PT" << value.Seconds() << "S')";
}

TString BuildCreateReplicationQuery(
        const TString& db,
        const TString& backupRoot,
        const TString& name,
        const NReplication::TReplicationDescription& desc)
{
    TVector<TString> targets(::Reserve(desc.GetItems().size()));
    for (const auto& item : desc.GetItems()) {
        if (!item.DstPath.ends_with("/indexImplTable")) { // TODO(ilnaz): get rid of this hack
            targets.push_back(BuildTarget(item.SrcPath.c_str(), item.DstPath.c_str()));
        }
    }

    const auto& params = desc.GetConnectionParams();

    TVector<TString> opts(::Reserve(5 /* max options */));
    opts.push_back(BuildOption("CONNECTION_STRING", Quote(BuildConnectionString(params))));
    switch (params.GetCredentials()) {
        case NReplication::TConnectionParams::ECredentials::Static:
            opts.push_back(BuildOption("USER", Quote(params.GetStaticCredentials().User)));
            opts.push_back(BuildOption("PASSWORD_SECRET_NAME", Quote(params.GetStaticCredentials().PasswordSecretName)));
            break;
        case NReplication::TConnectionParams::ECredentials::OAuth:
            opts.push_back(BuildOption("TOKEN_SECRET_NAME", Quote(params.GetOAuthCredentials().TokenSecretName)));
            break;
    }

    opts.push_back(BuildOption("CONSISTENCY_LEVEL", Quote(ToString(desc.GetConsistencyLevel()))));
    if (desc.GetConsistencyLevel() == NReplication::TReplicationDescription::EConsistencyLevel::Global) {
        opts.push_back(BuildOption("COMMIT_INTERVAL", Interval(desc.GetGlobalConsistency().GetCommitInterval())));
    }

    return std::format(
            "-- database: \"{}\"\n"
            "-- backup root: \"{}\"\n"
            "CREATE ASYNC REPLICATION `{}`\nFOR\n{}\nWITH (\n{}\n);",
        db.c_str(), backupRoot.c_str(), name.c_str(), JoinSeq(",\n", targets).c_str(), JoinSeq(",\n", opts).c_str());
}

}

void BackupReplication(
    TDriver driver,
    const TString& db,
    const TString& dbBackupRoot,
    const TString& dbPathRelativeToBackupRoot,
    const TFsPath& fsBackupFolder)
{
    Y_ENSURE(!dbPathRelativeToBackupRoot.empty());
    const auto dbPath = JoinDatabasePath(dbBackupRoot, dbPathRelativeToBackupRoot);

    LOG_I("Backup async replication " << dbPath.Quote() << " to " << fsBackupFolder.GetPath().Quote());

    const auto desc = DescribeReplication(driver, dbPath);
    const auto creationQuery = BuildCreateReplicationQuery(db, dbBackupRoot, fsBackupFolder.GetName(), desc);

    WriteCreationQueryToFile(creationQuery, fsBackupFolder, NDump::NFiles::CreateAsyncReplication());
    BackupPermissions(driver, dbPath, fsBackupFolder);
}

namespace {

Ydb::Table::DescribeExternalDataSourceResult DescribeExternalDataSource(TDriver driver, const TString& path) {
    NTable::TTableClient client(driver);
    Ydb::Table::DescribeExternalDataSourceResult description;
    auto status = client.RetryOperationSync([&](NTable::TSession session) {
        auto result = session.DescribeExternalDataSource(path).ExtractValueSync();
        if (result.IsSuccess()) {
            description = TProtoAccessor::GetProto(result.GetExternalDataSourceDescription());
        }
        return result;
    });
    VerifyStatus(status, "describe external data source");
    return description;
}

std::string ToString(std::string_view key, std::string_view value) {
    // indented to follow the default YQL formatting
    return std::format(R"(  {} = '{}')", key, value);
}

namespace NExternalDataSource {

    std::string PropertyToString(const std::pair<TProtoStringType, TProtoStringType>& property) {
        const auto& [key, value] = property;
        return ToString(key, value);
    }

}

TString BuildCreateExternalDataSourceQuery(const Ydb::Table::DescribeExternalDataSourceResult& description) {
    return std::format(
        "CREATE EXTERNAL DATA SOURCE IF NOT EXISTS `{}` WITH (\n{},\n{}{}\n);",
        description.self().name().c_str(),
        ToString("SOURCE_TYPE", description.source_type()),
        ToString("LOCATION", description.location()),
        description.properties().empty()
            ? ""
            : std::string(",\n") +
                JoinSeq(",\n", std::views::transform(description.properties(), NExternalDataSource::PropertyToString)).c_str()
    );
}

}

void BackupExternalDataSource(TDriver driver, const TString& dbPath, const TFsPath& fsBackupFolder) {
    Y_ENSURE(!dbPath.empty());
    LOG_I("Backup external data source " << dbPath.Quote() << " to " << fsBackupFolder.GetPath().Quote());

    const auto description = DescribeExternalDataSource(driver, dbPath);
    const auto creationQuery = BuildCreateExternalDataSourceQuery(description);

    WriteCreationQueryToFile(creationQuery, fsBackupFolder, NDump::NFiles::CreateExternalDataSource());
    BackupPermissions(driver, dbPath, fsBackupFolder);
}

namespace {

Ydb::Table::DescribeExternalTableResult DescribeExternalTable(TDriver driver, const TString& path) {
    NTable::TTableClient client(driver);
    Ydb::Table::DescribeExternalTableResult description;
    auto status = client.RetryOperationSync([&](NTable::TSession session) {
        auto result = session.DescribeExternalTable(path).ExtractValueSync();
        if (result.IsSuccess()) {
            description = TProtoAccessor::GetProto(result.GetExternalTableDescription());
        }
        return result;
    });
    VerifyStatus(status, "describe external table");
    return description;
}

namespace NExternalTable {

    std::string PropertyToString(const std::pair<TProtoStringType, TProtoStringType>& property) {
        const auto& [key, json] = property;
        const auto items = NJson::ReadJsonFastTree(json).GetArray();
        Y_ENSURE(!items.empty(), "Empty items for an external table property: " << key);
        if (items.size() == 1) {
            return ToString(key, items.front().GetString());
        } else {
            return ToString(key, std::format("[{}]", JoinSeq(", ", items).c_str()));
        }
    }

}

std::string ColumnToString(const Ydb::Table::ColumnMeta& column) {
    const auto& type = column.type();
    const bool notNull = !type.has_optional_type() || (type.has_pg_type() && column.not_null());
    return std::format(
        "    {} {}{}",
        column.name().c_str(),
        TType(type).ToString(),
        notNull ? " NOT NULL" : ""
    );
}

TString BuildCreateExternalTableQuery(const Ydb::Table::DescribeExternalTableResult& description) {
    return std::format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `{}` (\n{}\n) WITH (\n{},\n{}{}\n);",
        description.self().name().c_str(),
        JoinSeq(",\n", std::views::transform(description.columns(), ColumnToString)).c_str(),
        ToString("DATA_SOURCE", description.data_source_path()),
        ToString("LOCATION", description.location()),
        description.content().empty()
            ? ""
            : std::string(",\n") +
                JoinSeq(",\n", std::views::transform(description.content(), NExternalTable::PropertyToString)).c_str()
    );
}

}

void BackupExternalTable(TDriver driver, const TString& dbPath, const TFsPath& fsBackupFolder) {
    Y_ENSURE(!dbPath.empty());
    LOG_I("Backup external table " << dbPath.Quote() << " to " << fsBackupFolder.GetPath().Quote());

    const auto description = DescribeExternalTable(driver, dbPath);
    const auto creationQuery = BuildCreateExternalTableQuery(description);

    WriteCreationQueryToFile(creationQuery, fsBackupFolder, NDump::NFiles::CreateExternalTable());
    BackupPermissions(driver, dbPath, fsBackupFolder);
}

void CreateClusterDirectory(const TDriver& driver, const TString& path, bool rootBackupDir = false) {
    if (rootBackupDir) {
        LOG_I("Create temporary directory " << path.Quote() << " in database");
    } else {
        LOG_D("Create directory " << path.Quote() << " in database");
    }
    NScheme::TSchemeClient client(driver);
    TStatus status = client.MakeDirectory(path).GetValueSync();
    VerifyStatus(status, TStringBuilder() << "Create directory " << path.Quote() << " failed");
}

void RemoveClusterDirectory(const TDriver& driver, const TString& path) {
    LOG_D("Remove directory " << path.Quote());
    NScheme::TSchemeClient client(driver);
    TStatus status = client.RemoveDirectory(path).GetValueSync();
    VerifyStatus(status, TStringBuilder() << "Remove directory " << path.Quote() << " failed");
}

void RemoveClusterDirectoryRecursive(const TDriver& driver, const TString& path) {
    LOG_I("Remove temporary directory " << path.Quote() << " in database");
    NScheme::TSchemeClient schemeClient(driver);
    NTable::TTableClient tableClient(driver);
    TStatus status = NConsoleClient::RemoveDirectoryRecursive(schemeClient, tableClient, path, {}, true, false);
    VerifyStatus(status, TStringBuilder() << "Remove temporary directory " << path.Quote() << " failed");
}

static bool IsExcluded(const TString& path, const TVector<TRegExMatch>& exclusionPatterns) {
    for (const auto& pattern : exclusionPatterns) {
        if (pattern.Match(path.c_str())) {
            return true;
        }
    }

    return false;
}

static void MaybeCreateEmptyFile(const TFsPath& folderPath) {
    TVector<TString> children;
    folderPath.ListNames(children);
    if (children.empty() || (children.size() == 1 && children[0] == NDump::NFiles::Incomplete().FileName)) {
        TFile(folderPath.Child(NDump::NFiles::Empty().FileName), CreateAlways);
    }
}

void BackupFolderImpl(TDriver driver, const TString& database, const TString& dbPrefix, const TString& backupPrefix,
        const TFsPath folderPath, const TVector<TRegExMatch>& exclusionPatterns,
        bool schemaOnly, bool useConsistentCopyTable, bool avoidCopy, bool preservePoolKinds, bool ordered,
        NYql::TIssues& issues
) {
    TFile(folderPath.Child(NDump::NFiles::Incomplete().FileName), CreateAlways).Close();

    TMap<TString, TAsyncStatus> copiedTablesStatuses;
    TVector<NTable::TCopyItem> tablesToCopy;
    // Copy all tables to temporal folder and backup other scheme objects along the way.
    {
        TDbIterator<ETraverseType::Preordering> dbIt(driver, dbPrefix);
        while (dbIt) {
            if (IsExcluded(dbIt.GetFullPath(), exclusionPatterns)) {
                LOG_D("Skip " << dbIt.GetFullPath().Quote());
                dbIt.Next();
                continue;
            }

            auto childFolderPath = CreateDirectory(folderPath, dbIt.GetRelPath());
            TFile(childFolderPath.Child(NDump::NFiles::Incomplete().FileName), CreateAlways).Close();
            if (schemaOnly) {
                if (dbIt.IsTable()) {
                    BackupTable(driver, dbIt.GetTraverseRoot(), backupPrefix, dbIt.GetRelPath(),
                            childFolderPath, schemaOnly, preservePoolKinds, ordered);
                    childFolderPath.Child(NDump::NFiles::Incomplete().FileName).DeleteIfExists();
                }
            } else if (!avoidCopy) {
                if (dbIt.IsTable()) {
                    const TString tmpTablePath = JoinDatabasePath(backupPrefix, dbIt.GetRelPath());
                    if (useConsistentCopyTable) {
                        tablesToCopy.emplace_back(dbIt.GetFullPath(), tmpTablePath);
                    } else {
                        auto status = CopyTableAsyncStart(driver, dbIt.GetFullPath(), tmpTablePath);
                        copiedTablesStatuses.emplace(dbIt.GetFullPath(), std::move(status));
                    }
                } else if (dbIt.IsDir()) {
                    CreateClusterDirectory(driver, JoinDatabasePath(backupPrefix, dbIt.GetRelPath()));
                }
            }
            if (dbIt.IsView()) {
                BackupView(driver, dbIt.GetTraverseRoot(), dbIt.GetRelPath(), childFolderPath, issues);
            }
            if (dbIt.IsTopic()) {
                BackupTopic(driver, dbIt.GetFullPath(), childFolderPath);
            }
            if (dbIt.IsCoordinationNode()) {
                BackupCoordinationNode(driver, dbIt.GetFullPath(), childFolderPath);
            }
            if (dbIt.IsReplication()) {
                BackupReplication(driver, database, dbIt.GetTraverseRoot(), dbIt.GetRelPath(), childFolderPath);
            }
            if (dbIt.IsExternalDataSource()) {
                BackupExternalDataSource(driver, dbIt.GetFullPath(), childFolderPath);
            }
            if (dbIt.IsExternalTable()) {
                BackupExternalTable(driver, dbIt.GetFullPath(), childFolderPath);
            }
            dbIt.Next();
        }
    }

    if (schemaOnly) {
        TDbIterator<ETraverseType::Postordering> dbIt(driver, dbPrefix);
        while (dbIt) {
            if (IsExcluded(dbIt.GetFullPath(), exclusionPatterns)) {
                LOG_D("Skip " << dbIt.GetFullPath().Quote());
                dbIt.Next();
                continue;
            }

            TFsPath childFolderPath = folderPath.Child(dbIt.GetRelPath());
            if (dbIt.IsTable()) {
                // If table backup was not successful exception should be thrown,
                // so control flow can't reach this line. Check it just to be sure
                Y_ENSURE(!childFolderPath.Child(NDump::NFiles::Incomplete().FileName).Exists());
            } else if (dbIt.IsDir()) {
                MaybeCreateEmptyFile(childFolderPath);
                BackupPermissions(driver, dbIt.GetTraverseRoot(), dbIt.GetRelPath(), childFolderPath);
            }

            childFolderPath.Child(NDump::NFiles::Incomplete().FileName).DeleteIfExists();
            dbIt.Next();
        }
        folderPath.Child(NDump::NFiles::Incomplete().FileName).DeleteIfExists();
        return;
    }

    if (useConsistentCopyTable && !avoidCopy && tablesToCopy) {
        CopyTables(driver, tablesToCopy);
    }
    // Read all tables from temporal folder and delete them
    {
        TDbIterator<ETraverseType::Postordering> dbIt(driver, dbPrefix);
        while (dbIt) {
            if (IsExcluded(dbIt.GetFullPath(), exclusionPatterns)) {
                LOG_D("Skip " << dbIt.GetFullPath().Quote());
                dbIt.Next();
                continue;
            }

            TFsPath childFolderPath = folderPath.Child(dbIt.GetRelPath());
            const TString tmpTablePath = JoinDatabasePath(backupPrefix, dbIt.GetRelPath());

            if (dbIt.IsTable()) {
                if (!useConsistentCopyTable && !avoidCopy) {
                    Y_ENSURE(copiedTablesStatuses.contains(dbIt.GetFullPath()),
                            "Table was not copied but going to be backuped, path# " << dbIt.GetFullPath().Quote());
                    CopyTableAsyncFinish(copiedTablesStatuses[dbIt.GetFullPath()], dbIt.GetFullPath());
                    copiedTablesStatuses.erase(dbIt.GetFullPath());
                }
                BackupTable(driver, dbIt.GetTraverseRoot(), avoidCopy ? dbIt.GetTraverseRoot() : backupPrefix, dbIt.GetRelPath(),
                        childFolderPath, schemaOnly, preservePoolKinds, ordered);
                if (!avoidCopy) {
                    DropTable(driver, tmpTablePath);
                }
            } else if (dbIt.IsDir()) {
                MaybeCreateEmptyFile(childFolderPath);
                BackupPermissions(driver, dbIt.GetTraverseRoot(), dbIt.GetRelPath(), childFolderPath);
                if (!avoidCopy) {
                    RemoveClusterDirectory(driver, tmpTablePath);
                }
            }

            childFolderPath.Child(NDump::NFiles::Incomplete().FileName).DeleteIfExists();
            dbIt.Next();
        }
    }
    Y_ENSURE(copiedTablesStatuses.empty(), "Some tables was copied but not backuped, example of such table, path# "
            << copiedTablesStatuses.begin()->first.Quote());
    folderPath.Child(NDump::NFiles::Incomplete().FileName).DeleteIfExists();
}

namespace {

NCms::TListDatabasesResult ListDatabases(TDriver driver) {
    NCms::TCmsClient client(driver);

    auto status = NDump::ListDatabases(client);
    VerifyStatus(status);

    return status;
}

NCms::TGetDatabaseStatusResult GetDatabaseStatus(TDriver driver, const std::string& path) {
    NCms::TCmsClient client(driver);

    auto status = NDump::GetDatabaseStatus(client, path);
    VerifyStatus(status);

    return status;
}

bool IsNotLowerAlphaNum(char c) {
    if (isalnum(c)) {
        if (isalpha(c)) {
            return !islower(c);
        }

        return false;
    }

    return true;
}

bool IsValidSid(const std::string& sid) {
    return std::find_if(sid.begin(), sid.end(), IsNotLowerAlphaNum) == sid.end();
}

struct TAdmins {
    TString GroupSid;
    THashSet<TString> UserSids;
};

TAdmins FindAdmins(TDriver driver, const TString& dbPath) {
    THashSet<TString> adminUserSids;

    auto entry = DescribePath(driver, dbPath);

    NYdb::NTable::TTableClient client(driver);
    auto query = Sprintf("SELECT * FROM `%s/.sys/auth_group_members`", dbPath.c_str());
    auto settings = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();

    std::vector<TResultSet> resultSets;
    TStatus status = client.RetryOperationSync([&](NTable::TSession session) {
        auto result = session.ExecuteDataQuery(query, settings).ExtractValueSync();
        VerifyStatus(result);
        resultSets = result.GetResultSets();
        return result;
    });
    VerifyStatus(status);

    TStringStream alterGroupQuery;
    for (const auto& resultSet : resultSets) {
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto groupSidValue = parser.GetValue("GroupSid");
            auto memberSidValue = parser.GetValue("MemberSid");

            const auto& groupSid = groupSidValue.GetProto().text_value();
            const auto& memberSid = memberSidValue.GetProto().text_value();

            if (groupSid == entry.Owner) {
                adminUserSids.insert(memberSid);
            }
        }
    }

    return { TString(entry.Owner), adminUserSids };
}

struct TBackupDatabaseSettings {
    bool WithRegularUsers = false;
    bool WithContent = false;
    TString TemporalBackupPostfix = "";
};

void BackupUsers(TDriver driver, const TString& dbPath, const TFsPath& folderPath, const THashSet<TString>& filter = {}) {
    NYdb::NTable::TTableClient client(driver);
    auto query = Sprintf("SELECT * FROM `%s/.sys/auth_users`", dbPath.c_str());
    auto settings = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();

    std::vector<TResultSet> resultSets;
    TStatus status = client.RetryOperationSync([&](NTable::TSession session) {
        auto result = session.ExecuteDataQuery(query, settings).ExtractValueSync();
        VerifyStatus(result);
        resultSets = result.GetResultSets();
        return result;
    });
    VerifyStatus(status);

    TStringStream createUserQuery;
    for (const auto& resultSet : resultSets) {
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto sidValue = parser.GetValue("Sid");
            auto passwordValue = parser.GetValue("PasswordHash");
            auto isEnabledValue = parser.GetValue("IsEnabled");

            const auto& sid = sidValue.GetProto().text_value();
            const auto& password = passwordValue.GetProto().text_value();
            bool isEnabled = isEnabledValue.GetProto().bool_value();

            if (!filter.empty() && !filter.contains(sid)) {
                continue;
            }

            // Some SIDs may be created through configuration that bypasses this checks
            if (IsValidSid(sid)) {
                createUserQuery << Sprintf("CREATE USER `%s` HASH '%s';\n", sid.c_str(), password.c_str());
            }

            if (!isEnabled) {
                createUserQuery << Sprintf("ALTER USER `%s` NOLOGIN;\n", sid.c_str());
            }
        }
    }

    WriteCreationQueryToFile(createUserQuery.Str(), folderPath, NDump::NFiles::CreateUser());
}

void BackupGroups(TDriver driver, const TString& dbPath, const TFsPath& folderPath, const THashSet<TString>& filter = {}) {
    NYdb::NTable::TTableClient client(driver);
    auto query = Sprintf("SELECT * FROM `%s/.sys/auth_groups`", dbPath.c_str());
    auto settings = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();

    std::vector<TResultSet> resultSets;
    TStatus status = client.RetryOperationSync([&](NTable::TSession session) {
        auto result = session.ExecuteDataQuery(query, settings).ExtractValueSync();
        VerifyStatus(result);
        resultSets = result.GetResultSets();
        return result;
    });
    VerifyStatus(status);

    TStringStream createGroupQuery;
    for (const auto& resultSet : resultSets) {
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto sidValue = parser.GetValue("Sid");
            const auto& sid = sidValue.GetProto().text_value();

            if (!filter.empty() && !filter.contains(sid)) {
                continue;
            }

            // Some SIDs may be created through configuration that bypasses this checks
            if (IsValidSid(sid)) {
                createGroupQuery << Sprintf("CREATE GROUP `%s`;\n", sid.c_str());
            }
        }
    }

    WriteCreationQueryToFile(createGroupQuery.Str(), folderPath, NDump::NFiles::CreateGroup());
}

void BackupGroupMembers(TDriver driver, const TString& dbPath, const TFsPath& folderPath, const THashSet<TString>& filterGroups = {}) {
    NYdb::NTable::TTableClient client(driver);
    auto query = Sprintf("SELECT * FROM `%s/.sys/auth_group_members`", dbPath.c_str());
    auto settings = NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx();

    std::vector<TResultSet> resultSets;
    TStatus status = client.RetryOperationSync([&](NTable::TSession session) {
        auto result = session.ExecuteDataQuery(query, settings).ExtractValueSync();
        VerifyStatus(result);
        resultSets = result.GetResultSets();
        return result;
    });
    VerifyStatus(status);

    TStringStream alterGroupQuery;
    for (const auto& resultSet : resultSets) {
        TResultSetParser parser(resultSet);
        while (parser.TryNextRow()) {
            auto groupSidValue = parser.GetValue("GroupSid");
            auto memberSidValue = parser.GetValue("MemberSid");

            const auto& groupSid = groupSidValue.GetProto().text_value();
            const auto& memberSid = memberSidValue.GetProto().text_value();

            if (!filterGroups.empty() && !filterGroups.contains(groupSid)) {
                continue;
            }

            alterGroupQuery << Sprintf("ALTER GROUP `%s` ADD USER `%s`;\n", groupSid.c_str(), memberSid.c_str());
        }
    }

    WriteCreationQueryToFile(alterGroupQuery.Str(), folderPath, NDump::NFiles::AlterGroup());
}

void BackupDatabaseImpl(TDriver driver, const TString& dbPath, const TFsPath& folderPath, TBackupDatabaseSettings settings) {
    LOG_I("Backup database " << dbPath.Quote() << " to " << folderPath.GetPath().Quote());
    folderPath.MkDirs();

    auto status = GetDatabaseStatus(driver, dbPath);
    Ydb::Cms::CreateDatabaseRequest proto;
    status.SerializeTo(proto);
    WriteProtoToFile(proto, folderPath, NDump::NFiles::Database());

    if (!settings.WithRegularUsers) {
        TAdmins admins = FindAdmins(driver, dbPath);
        BackupUsers(driver, dbPath, folderPath, admins.UserSids);
        BackupGroups(driver, dbPath, folderPath, { admins.GroupSid });
        BackupGroupMembers(driver, dbPath, folderPath, { admins.GroupSid });
    } else {
        BackupUsers(driver, dbPath, folderPath);
        BackupGroups(driver, dbPath, folderPath);
        BackupGroupMembers(driver, dbPath, folderPath);
    }

    BackupPermissions(driver, dbPath, "", folderPath);
    if (settings.WithContent) {
        // full path to temporal directory in database
        TString tmpDbFolder;
        try {
            tmpDbFolder = JoinDatabasePath(dbPath, "~" + settings.TemporalBackupPostfix);
            CreateClusterDirectory(driver, tmpDbFolder, true);

            NYql::TIssues issues;
            BackupFolderImpl(
                driver,
                dbPath,
                dbPath,
                tmpDbFolder,
                folderPath,
                /* exclusionPatterns */ {},
                /* schemaOnly */ false,
                /* useConsistentCopyTable */ true,
                /* avoidCopy */ false,
                /* preservePoolKinds */ false,
                /* ordered */ false,
                issues
            );

            if (issues) {
                Cerr << issues.ToString();
            }
        } catch (...) {
            RemoveClusterDirectoryRecursive(driver, tmpDbFolder);
            folderPath.ForceDelete();
            throw;
        }
        RemoveClusterDirectoryRecursive(driver, tmpDbFolder);
    }
}

TString FindClusterRootPath(TDriver driver) {
    NScheme::TSchemeClient client(driver);
    auto status = NDump::ListDirectory(client, "/");
    VerifyStatus(status);

    Y_ENSURE(status.GetChildren().size() == 1, "Exactly one cluster root expected, found: " << JoinSeq(", ", status.GetChildren()));
    return "/" + status.GetChildren().begin()->Name;
}

void BackupClusterRoot(TDriver driver, const TFsPath& folderPath) {
    TString rootPath = FindClusterRootPath(driver);

    LOG_I("Backup cluster root " << rootPath.Quote() << " to " << folderPath);

    BackupUsers(driver, rootPath, folderPath);
    BackupGroups(driver, rootPath, folderPath);
    BackupGroupMembers(driver, rootPath, folderPath);
    BackupPermissions(driver, rootPath, "", folderPath);
}

} // anonymous namespace

void CheckedCreateBackupFolder(const TFsPath& folderPath) {
    const bool exists = folderPath.Exists();
    if (exists) {
        TVector<TString> children;
        folderPath.ListNames(children);
        Y_ENSURE(children.empty(), "backup folder: " << folderPath.GetPath().Quote()
                << " should either not exists or be empty");
    } else {
        folderPath.MkDir();
    }
}

// relDbPath - relative path to directory/table to be backuped
// folderPath - relative path to folder in local filesystem where backup will be stored
void BackupFolder(const TDriver& driver, const TString& database, const TString& relDbPath, TFsPath folderPath,
        const TVector<TRegExMatch>& exclusionPatterns,
        bool schemaOnly, bool useConsistentCopyTable, bool avoidCopy, bool savePartialResult, bool preservePoolKinds, bool ordered) {
    TString temporalBackupPostfix = CreateTemporalBackupName();
    if (!folderPath) {
        folderPath = temporalBackupPostfix;
    }
    CheckedCreateBackupFolder(folderPath);

    TString dbPrefix = JoinDatabasePath(database, relDbPath);
    LOG_I("Backup " << dbPrefix.Quote() << " to " << folderPath.GetPath().Quote());

    // full path to temporal directory in database
    TString tmpDbFolder;
    try {
        if (!schemaOnly && !avoidCopy) {
            // Create temporal folder in database's root directory
            tmpDbFolder = JoinDatabasePath(database, "~" + temporalBackupPostfix);
            CreateClusterDirectory(driver, tmpDbFolder, true);
        }

        NYql::TIssues issues;
        BackupFolderImpl(driver, database, dbPrefix, tmpDbFolder, folderPath, exclusionPatterns,
            schemaOnly, useConsistentCopyTable, avoidCopy, preservePoolKinds, ordered, issues
        );

        if (issues) {
            Cerr << issues.ToString();
        }
    } catch (...) {
        if (!schemaOnly && !avoidCopy) {
            RemoveClusterDirectoryRecursive(driver, tmpDbFolder);
        }

        LOG_E("Backup failed");
        // delete partial backup (or save)
        if (!savePartialResult) {
            folderPath.ForceDelete();
        } else {
            LOG_I("Partial result saved");
        }

        throw;
    }
    if (!schemaOnly && !avoidCopy) {
        RemoveClusterDirectoryRecursive(driver, tmpDbFolder);
    }
    LOG_I("Backup completed successfully");
}

void BackupDatabase(const TDriver& driver, const TString& database, TFsPath folderPath) {
    TString temporalBackupPostfix = CreateTemporalBackupName();
    if (!folderPath) {
        folderPath = temporalBackupPostfix;
    }
    CheckedCreateBackupFolder(folderPath);

    try {
        NYql::TIssues issues;
        TFile(folderPath.Child(NDump::NFiles::Incomplete().FileName), CreateAlways).Close();

        BackupDatabaseImpl(driver, database, folderPath, {
            .WithRegularUsers = true,
            .WithContent = true,
            .TemporalBackupPostfix = temporalBackupPostfix,
        });

        folderPath.Child(NDump::NFiles::Incomplete().FileName).DeleteIfExists();
        if (issues) {
            Cerr << issues.ToString();
        }
    } catch (...) {
        LOG_E("Backup failed");
        folderPath.ForceDelete();
        throw;
    }
    LOG_I("Backup database " << database.Quote() << " is completed successfully");
}

void BackupCluster(const TDriver& driver, TFsPath folderPath) {
    TString temporalBackupPostfix = CreateTemporalBackupName();
    if (!folderPath) {
        folderPath = temporalBackupPostfix;
    }
    CheckedCreateBackupFolder(folderPath);

    LOG_I("Backup cluster to " << folderPath.GetPath().Quote());

    try {
        NYql::TIssues issues;
        TFile(folderPath.Child(NDump::NFiles::Incomplete().FileName), CreateAlways).Close();

        BackupClusterRoot(driver, folderPath);
        auto databases = ListDatabases(driver);
        for (const auto& database : databases.GetPaths()) {
            BackupDatabaseImpl(driver, TString(database), folderPath.Child("." + database), {
                .WithRegularUsers = false,
                .WithContent = false,
            });
        }

        folderPath.Child(NDump::NFiles::Incomplete().FileName).DeleteIfExists();
        if (issues) {
            Cerr << issues.ToString();
        }
    } catch (...) {
        LOG_E("Backup failed");
        folderPath.ForceDelete();
        throw;
    }
    LOG_I("Backup cluster is completed successfully");
}

} //  NYdb::NBackup
