#include "backup.h"
#include "db_iterator.h"
#include "util.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>
#include <ydb/public/lib/ydb_cli/common/retry_func.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/regex/pcre/regexp.h>
#include <library/cpp/string_utils/quote/quote.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/folder/pathsplit.h>
#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/stream/format.h>
#include <util/stream/mem.h>
#include <util/stream/null.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/fs.h>

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
        TString str = parser.Get##type();           \
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
            LOG_D("Retry read table from key: " << FormatValueYson(*lastWrittenPK).Quote());
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

void BackupPermissions(TDriver driver, const TString& dbPrefix, const TString& path, const TFsPath& folderPath) {
    auto entry = DescribePath(driver, JoinDatabasePath(dbPrefix, path));
    Ydb::Scheme::ModifyPermissionsRequest proto;
    entry.SerializeTo(proto);
    WriteProtoToFile(proto, folderPath, NDump::NFiles::Permissions());
}

Ydb::Table::ChangefeedDescription ProtoFromChangefeedDesc(const NTable::TChangefeedDescription& changefeedDesc) {
    Ydb::Table::ChangefeedDescription protoChangeFeedDesc;
    changefeedDesc.SerializeTo(protoChangeFeedDesc);
    return protoChangeFeedDesc;
}

NTopic::TDescribeTopicResult DescribeTopic(TDriver driver, const TString& path) {
    NYdb::NTopic::TTopicClient client(driver);
    return NConsoleClient::RetryFunction([&]() {
        return client.DescribeTopic(path).GetValueSync();
    });
}

void BackupChangefeeds(TDriver driver, const TString& tablePath, const TFsPath& folderPath) {
    
    auto desc = DescribeTable(driver, tablePath);

    for (const auto& changefeedDesc : desc.GetChangefeedDescriptions()) {
        TFsPath changefeedDirPath = CreateDirectory(folderPath, changefeedDesc.GetName());
        
        auto protoChangeFeedDesc = ProtoFromChangefeedDesc(changefeedDesc);
        const auto descTopicResult = DescribeTopic(driver, JoinDatabasePath(tablePath, changefeedDesc.GetName()));
        VerifyStatus(descTopicResult);
        const auto& topicDescription = descTopicResult.GetTopicDescription();
        const auto protoTopicDescription = NYdb::TProtoAccessor::GetProto(topicDescription);

        WriteProtoToFile(protoChangeFeedDesc, changefeedDirPath, NDump::NFiles::Changefeed());
        WriteProtoToFile(protoTopicDescription, changefeedDirPath, NDump::NFiles::Topic());
    }
}

void BackupTable(TDriver driver, const TString& dbPrefix, const TString& backupPrefix, const TString& path,
        const TFsPath& folderPath, bool schemaOnly, bool preservePoolKinds, bool ordered) {
    Y_ENSURE(!path.empty());
    Y_ENSURE(path.back() != '/', path.Quote() << " path contains / in the end");

    const auto fullPath = JoinDatabasePath(schemaOnly ? dbPrefix : backupPrefix, path);
    const auto tablePath = JoinDatabasePath(dbPrefix, path);

    LOG_I("Backup table " << fullPath.Quote() << " to " << folderPath.GetPath().Quote());

    auto desc = DescribeTable(driver, fullPath);
    auto proto = ProtoFromTableDescription(desc, preservePoolKinds);
    WriteProtoToFile(proto, folderPath, NDump::NFiles::TableScheme());
  
    BackupChangefeeds(driver, tablePath, folderPath);
    BackupPermissions(driver, dbPrefix, path, folderPath);

    if (!schemaOnly) {
        ReadTable(driver, desc, fullPath, folderPath, ordered);
    }
}

void CreateClusterDirectory(const TDriver& driver, const TString& path, bool rootBackupDir = false) {
    if (rootBackupDir) {
        LOG_I("Create temporary directory " << path.Quote());
    } else {
        LOG_D("Create directory " << path.Quote());
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
    LOG_I("Remove temporary directory " << path.Quote());
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

void BackupFolderImpl(TDriver driver, const TString& dbPrefix, const TString& backupPrefix,
        const TFsPath folderPath, const TVector<TRegExMatch>& exclusionPatterns,
        bool schemaOnly, bool useConsistentCopyTable, bool avoidCopy, bool preservePoolKinds, bool ordered) {
    TFile(folderPath.Child(NDump::NFiles::Incomplete().FileName), CreateAlways);

    TMap<TString, TAsyncStatus> copiedTablesStatuses;
    TVector<NTable::TCopyItem> tablesToCopy;
    // Copy all tables to temporal folder
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

        BackupFolderImpl(driver, dbPrefix, tmpDbFolder, folderPath, exclusionPatterns,
            schemaOnly, useConsistentCopyTable, avoidCopy, preservePoolKinds, ordered);
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

} //  NYdb::NBackup
