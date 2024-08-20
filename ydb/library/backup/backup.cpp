#include "backup.h"
#include "db_iterator.h"
#include "query_builder.h"
#include "query_uploader.h"
#include "util.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/lib/yson_value/ydb_yson_value.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/ydb_cli/common/recursive_remove.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
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
#include <util/string/printf.h>
#include <util/system/file.h>
#include <util/system/fs.h>

namespace NYdb::NBackup {


static constexpr const char *SCHEME_FILE_NAME = "scheme.pb";
static constexpr const char *PERMISSIONS_FILE_NAME = "permissions.pb";
static constexpr const char *INCOMPLETE_DATA_FILE_NAME = "incomplete.csv";
static constexpr const char *INCOMPLETE_FILE_NAME = "incomplete";
static constexpr const char *EMPTY_FILE_NAME = "empty_dir";

static constexpr size_t IO_BUFFER_SIZE = 2 << 20; // 2 MiB
static constexpr i64 FILE_SPLIT_THRESHOLD = 128 << 20; // 128 MiB
static constexpr i64 READ_TABLE_RETRIES = 100;


////////////////////////////////////////////////////////////////////////////////
//                               Util
////////////////////////////////////////////////////////////////////////////////

void TYdbErrorException::LogToStderr() const {
    LOG_ERR("Ydb error, status# " << Status.GetStatus());
    if (what()) {
        LOG_ERR("\t" << "What# " << what());
    }
    LOG_ERR("\t" << Status.GetIssues().ToString());
}

static void VerifyStatus(TStatus status, TString explain = "") {
    if (status.IsSuccess()) {
        if (status.GetIssues()) {
            LOG_DEBUG(status.GetIssues().ToString());
        }
    } else {
        throw TYdbErrorException(status) << explain;
    }
}

static TString NameFromDbPath(const TString& path) {
    TPathSplitUnix split(path);
    return TString{split.back()};
}

static TString ParentPathFromDbPath(const TString& path) {
    TPathSplitUnix split(path);
    split.pop_back();
    return split.Reconstruct();
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
        TResultSetParser resultSetParser, TFile* dataFile, const NTable::TTableDescription* desc) {
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

    LOG_DEBUG("New file with data is created, fileName# " << fileName);
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
        VerifyStatus(result, TStringBuilder() << "ReadTable result was not successfull,"
                " path: " << fullTablePath.Quote());
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
        VerifyStatus(resultSetStreamPart, TStringBuilder() << "TStreamPart<TResultSet> is not successfull"
                    " error msg: " << resultSetStreamPart.GetIssues().ToString());
        TResultSet resultSetCurrent = resultSetStreamPart.ExtractPart();

        auto tmpFile = TFile(folderPath.Child(INCOMPLETE_DATA_FILE_NAME), CreateAlways | WrOnly);
        TStringStream ss;
        ss.Reserve(IO_BUFFER_SIZE);

        TMaybe<TValue> lastReadPK;

        while (true) {
            NTable::TAsyncSimpleStreamPart<TResultSet> nextResult;
            if (resultSetCurrent.Truncated()) {
                nextResult = iter->ReadNext();
            }
            lastReadPK = ProcessResultSet(ss, resultSetCurrent, &tmpFile, &desc);

            // Next
            if (resultSetCurrent.Truncated()) {
                auto resultSetStreamPart = nextResult.ExtractValueSync();
                if (!resultSetStreamPart.IsSuccess()) {
                    LOG_DEBUG("resultSetStreamPart is not successful, EOS# "
                        << (resultSetStreamPart.EOS() ? "true" : "false"));
                    if (resultSetStreamPart.EOS()) {
                        break;
                    } else {
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
                tmpFile = TFile(folderPath.Child(INCOMPLETE_DATA_FILE_NAME), CreateAlways | WrOnly);
            }
        }
        Flush(tmpFile, ss, lastWrittenPK, lastReadPK);
        CloseAndRename(tmpFile, folderPath.Child(CreateDataFileName((*fileCounter)++)));
    }

    return {};
}

void ReadTable(TDriver driver, const NTable::TTableDescription& desc, const TString& fullTablePath,
        const TFsPath& folderPath, bool ordered) {
    LOG_DEBUG("Going to ReadTable, fullPath: " << fullTablePath);

    auto timer = GetVerbosity()
        ? MakeHolder<TScopedTimer>(TStringBuilder() << "Done read table# " << fullTablePath.Quote() << " took# ")
        : nullptr;

    TMaybe<TValue> lastWrittenPK;

    i64 retries = READ_TABLE_RETRIES;
    ui32 fileCounter = 0;
    do {
        lastWrittenPK = TryReadTable(driver, desc, fullTablePath, folderPath, lastWrittenPK, &fileCounter, ordered);
        if (lastWrittenPK && retries) {
            LOG_DEBUG("ReadTable was not successfull, going to retry from lastWrittenPK# "
                << FormatValueYson(*lastWrittenPK).Quote());
        }
    } while (lastWrittenPK && retries--);

    Y_ENSURE(!lastWrittenPK, "For table " << fullTablePath.Quote() << " ReadTable hasn't finished successfully after "
            << READ_TABLE_RETRIES << " retries");
}

NTable::TTableDescription DescribeTable(TDriver driver, const TString& fullTablePath) {
    TMaybe<NTable::TTableDescription> desc;

    NTable::TTableClient client(driver);

    TStatus status = client.RetryOperationSync([fullTablePath, &desc](NTable::TSession session) {
        auto settings = NTable::TDescribeTableSettings().WithKeyShardBoundary(true);
        auto result = session.DescribeTable(fullTablePath, settings).GetValueSync();

        VerifyStatus(result);
        desc = result.GetTableDescription();
        return result;
    });
    VerifyStatus(status);
    LOG_DEBUG("Table is described, fullPath: " << fullTablePath);

    for (auto& column : desc->GetColumns()) {
        LOG_DEBUG("Column, name: " << column.Name << ", type: " << FormatType(column.Type));
    }
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

    auto status = client.DescribePath(fullPath).GetValueSync();
    VerifyStatus(status);
    LOG_DEBUG("Path is described, fullPath: " << fullPath);

    return status.GetEntry();
}

TAsyncStatus CopyTableAsyncStart(TDriver driver, const TString& src, const TString& dst) {
    NTable::TTableClient client(driver);

    return client.RetryOperation([src, dst](NTable::TSession session) {
        auto result = session.CopyTable(src, dst);

        return result;
    });
}

void CopyTableAsyncFinish(const TAsyncStatus& status, const TString& src, const TString& dst) {
    VerifyStatus(status.GetValueSync(), TStringBuilder() << "CopyTable, src: " << src.Quote() << " dst: " << dst.Quote());
    LOG_DEBUG("Table is copied, src: " << src.Quote() << " dst: " << dst.Quote());
}

void CopyTables(TDriver driver, const TVector<NTable::TCopyItem>& tablesToCopy) {
    NTable::TTableClient client(driver);

    TStatus status = client.RetryOperationSync([&tablesToCopy](NTable::TSession session) {
        auto result = session.CopyTables(tablesToCopy).GetValueSync();

        return result;
    });

    // Debug print
    TStringStream tablesStr;
    bool needsComma = false;
    for (const auto& copyItem : tablesToCopy) {
        if (needsComma) {
            tablesStr << ", ";
        } else {
            needsComma = true;
        }
        tablesStr << "{ src# " << copyItem.SourcePath() << ", dst# " << copyItem.DestinationPath().Quote() << "}";
    }

    VerifyStatus(status, TStringBuilder() << "CopyTables error, tables to be copied# " << tablesStr.Str());
    LOG_DEBUG("Tables are copied, " << tablesStr.Str());
}

void DropTable(TDriver driver, const TString& path) {
    NTable::TTableClient client(driver);
    TStatus status = client.RetryOperationSync([path](NTable::TSession session) {
        auto result = session.DropTable(path).GetValueSync();

        return result;
    });
    VerifyStatus(status, TStringBuilder() << "DropTable, path" << path.Quote());
    LOG_DEBUG("Table is dropped, path: " << path.Quote());
}

void BackupPermissions(TDriver driver, const TString& dbPrefix, const TString& path, const TFsPath& folderPath) {
    auto entry = DescribePath(driver, JoinDatabasePath(dbPrefix, path));
    Ydb::Scheme::ModifyPermissionsRequest proto;
    entry.SerializeTo(proto);

    TString permissionsStr;
    google::protobuf::TextFormat::PrintToString(proto, &permissionsStr);
    LOG_DEBUG("ModifyPermissionsRequest.proto: " << permissionsStr);

    TFile outFile(folderPath.Child(PERMISSIONS_FILE_NAME), CreateAlways | WrOnly);
    outFile.Write(permissionsStr.data(), permissionsStr.size());
}

void BackupTable(TDriver driver, const TString& dbPrefix, const TString& backupPrefix, const TString& path,
        const TFsPath& folderPath, bool schemaOnly, bool preservePoolKinds, bool ordered) {
    Y_ENSURE(!path.empty());
    Y_ENSURE(path.back() != '/', path.Quote() << " path contains / in the end");

    LOG_DEBUG("Going to backup table, dbPrefix: " << dbPrefix
        << " backupPrefix: " << backupPrefix << " path: " << path);

    auto desc = DescribeTable(driver, JoinDatabasePath(schemaOnly ? dbPrefix : backupPrefix, path));
    auto proto = ProtoFromTableDescription(desc, preservePoolKinds);

    TString schemaStr;
    google::protobuf::TextFormat::PrintToString(proto, &schemaStr);
    LOG_DEBUG("CreateTableRequest.proto: " << schemaStr);
    TFile outFile(folderPath.Child(SCHEME_FILE_NAME), CreateAlways | WrOnly);
    outFile.Write(schemaStr.data(), schemaStr.size());

    BackupPermissions(driver, dbPrefix, path, folderPath);

    if (!schemaOnly) {
        const TString pathToTemporal = JoinDatabasePath(backupPrefix, path);
        ReadTable(driver, desc, pathToTemporal, folderPath, ordered);
    }
}

void CreateClusterDirectory(const TDriver& driver, const TString& path) {
    NScheme::TSchemeClient client(driver);
    TStatus status = client.MakeDirectory(path).GetValueSync();
    VerifyStatus(status, TStringBuilder() << "MakeDirectory, path: " << path.Quote());
    LOG_DEBUG("Directory is created, path: " << path.Quote());
}

void RemoveClusterDirectory(const TDriver& driver, const TString& path) {
    NScheme::TSchemeClient client(driver);
    TStatus status = client.RemoveDirectory(path).GetValueSync();
    VerifyStatus(status, TStringBuilder() << "RemoveDirectory, path: " << path.Quote());
    LOG_DEBUG("Directory is removed, path: " << path.Quote());
}

void RemoveClusterDirectoryRecursive(const TDriver& driver, const TString& path) {
    NScheme::TSchemeClient schemeClient(driver);
    NTable::TTableClient tableClient(driver);
    TStatus status = NConsoleClient::RemoveDirectoryRecursive(schemeClient, tableClient, path, {}, true, false);
    VerifyStatus(status, TStringBuilder() << "RemoveDirectoryRecursive, path: " << path.Quote());
    LOG_DEBUG("Directory is removed recursively, path: " << path.Quote());
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
    if (children.empty() || (children.size() == 1 && children[0] == INCOMPLETE_FILE_NAME)) {
        TFile(folderPath.Child(EMPTY_FILE_NAME), CreateAlways);
    }
}

void BackupFolderImpl(TDriver driver, const TString& dbPrefix, const TString& backupPrefix, TString path,
        const TFsPath folderPath, const TVector<TRegExMatch>& exclusionPatterns,
        bool schemaOnly, bool useConsistentCopyTable, bool avoidCopy, bool preservePoolKinds, bool ordered) {
    LOG_DEBUG("Going to backup folder/table, dbPrefix: " << dbPrefix << " path: " << path);
    TFile(folderPath.Child(INCOMPLETE_FILE_NAME), CreateAlways);

    TMap<TString, TAsyncStatus> copiedTablesStatuses;
    TVector<NTable::TCopyItem> tablesToCopy;
    // Copy all tables to temporal folder
    {
        TDbIterator<ETraverseType::Preordering> dbIt(driver, dbPrefix);
        while (dbIt) {
            if (IsExcluded(dbIt.GetFullPath(), exclusionPatterns)) {
                LOG_DEBUG("skip path# " << dbIt.GetFullPath());
                dbIt.Next();
                continue;
            }

            TFsPath childFolderPath = folderPath.Child(dbIt.GetRelPath());
            LOG_DEBUG("path to backup# " << childFolderPath.GetPath());
            childFolderPath.MkDir();
            TFile(childFolderPath.Child(INCOMPLETE_FILE_NAME), CreateAlways).Close();
            if (schemaOnly) {
                if (dbIt.IsTable()) {
                    BackupTable(driver, dbIt.GetTraverseRoot(), backupPrefix, dbIt.GetRelPath(),
                            childFolderPath, schemaOnly, preservePoolKinds, ordered);
                    childFolderPath.Child(INCOMPLETE_FILE_NAME).DeleteIfExists();
                } else if (dbIt.IsDir()) {
                    BackupPermissions(driver, dbIt.GetTraverseRoot(), dbIt.GetRelPath(), childFolderPath);
                    childFolderPath.Child(INCOMPLETE_FILE_NAME).DeleteIfExists();
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
                dbIt.Next();
                continue;
            }

            TFsPath childFolderPath = folderPath.Child(dbIt.GetRelPath());
            if (dbIt.IsTable()) {
                // If table backup was not successful exception should be thrown,
                // so control flow can't reach this line. Check it just to be sure
                Y_ENSURE(!childFolderPath.Child(INCOMPLETE_FILE_NAME).Exists());
            } else if (dbIt.IsDir()) {
                MaybeCreateEmptyFile(childFolderPath);
            }

            childFolderPath.Child(INCOMPLETE_FILE_NAME).DeleteIfExists();
            dbIt.Next();
        }
        folderPath.Child(INCOMPLETE_FILE_NAME).DeleteIfExists();
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
                dbIt.Next();
                continue;
            }

            TFsPath childFolderPath = folderPath.Child(dbIt.GetRelPath());
            const TString tmpTablePath = JoinDatabasePath(backupPrefix, dbIt.GetRelPath());

            if (dbIt.IsTable()) {
                if (!useConsistentCopyTable && !avoidCopy) {
                    // CopyTableAsyncFinish(const TAsyncStatus& status, const TString& src, const TString& dst);
                    Y_ENSURE(copiedTablesStatuses.contains(dbIt.GetFullPath()),
                            "Table was not copied but going to be backuped, path# " << dbIt.GetFullPath().Quote());
                    CopyTableAsyncFinish(copiedTablesStatuses[dbIt.GetFullPath()], dbIt.GetFullPath(), tmpTablePath);
                    copiedTablesStatuses.erase(dbIt.GetFullPath());
                }
                BackupTable(driver, dbIt.GetTraverseRoot(), avoidCopy ? dbIt.GetTraverseRoot() : backupPrefix, dbIt.GetRelPath(),
                        childFolderPath, schemaOnly, preservePoolKinds, ordered);
                if (!avoidCopy) {
                    DropTable(driver, tmpTablePath);
                }
            } else if (dbIt.IsDir()) {
                BackupPermissions(driver, dbIt.GetTraverseRoot(), dbIt.GetRelPath(), childFolderPath);
                MaybeCreateEmptyFile(childFolderPath);
                if (!avoidCopy) {
                    RemoveClusterDirectory(driver, tmpTablePath);
                }
            }

            childFolderPath.Child(INCOMPLETE_FILE_NAME).DeleteIfExists();
            dbIt.Next();
        }
    }
    Y_ENSURE(copiedTablesStatuses.empty(), "Some tables was copied but not backuped, example of such table, path# "
            << copiedTablesStatuses.begin()->first.Quote());
    folderPath.Child(INCOMPLETE_FILE_NAME).DeleteIfExists();
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
    LOG_DEBUG("Going to backup into folder: " << folderPath.RealPath().GetPath().Quote());
}

// relDbPath - relative path to directory/table to be backuped
// folderPath - relative path to folder in local filesystem where backup will be stored
void BackupFolder(TDriver driver, const TString& database, const TString& relDbPath, TFsPath folderPath,
        const TVector<TRegExMatch>& exclusionPatterns,
        bool schemaOnly, bool useConsistentCopyTable, bool avoidCopy, bool savePartialResult, bool preservePoolKinds, bool ordered) {
    TString temporalBackupPostfix = CreateTemporalBackupName();
    if (!folderPath) {
        folderPath = temporalBackupPostfix;
    }
    CheckedCreateBackupFolder(folderPath);

    // full path to temporal directory in database
    TString tmpDbFolder;
    try {
        if (!schemaOnly && !avoidCopy) {
            // Create temporal folder in database's root directory
            tmpDbFolder = JoinDatabasePath(database, "~" + temporalBackupPostfix);
            CreateClusterDirectory(driver, tmpDbFolder);
        }

        TString dbPrefix = JoinDatabasePath(database, relDbPath);
        TString path;
        BackupFolderImpl(driver, dbPrefix, tmpDbFolder, path, folderPath, exclusionPatterns,
            schemaOnly, useConsistentCopyTable, avoidCopy, preservePoolKinds, ordered);
    } catch (...) {
        if (!schemaOnly && !avoidCopy) {
            RemoveClusterDirectoryRecursive(driver, tmpDbFolder);
        }

        // delete partial backup (or save)
        if (!savePartialResult) {
            folderPath.ForceDelete();
        }
        throw;
    }
    if (!schemaOnly && !avoidCopy) {
        RemoveClusterDirectoryRecursive(driver, tmpDbFolder);
    }
}

////////////////////////////////////////////////////////////////////////////////
//                               Restore
////////////////////////////////////////////////////////////////////////////////

TString ProcessColumnType(const TString& name, TTypeParser parser, NTable::TTableBuilder *builder) {
    TStringStream ss;
    ss << "name: " << name << "; ";
    if (parser.GetKind() == TTypeParser::ETypeKind::Optional) {
        ss << " optional; ";
        parser.OpenOptional();
    }
    ss << "kind: " << parser.GetKind() << "; ";
    switch (parser.GetKind()) {
        case TTypeParser::ETypeKind::Primitive:
            ss << " type_id: " << parser.GetPrimitive() << "; ";
            if (builder) {
                builder->AddNullableColumn(name, parser.GetPrimitive());
            }
            break;
        case TTypeParser::ETypeKind::Decimal:
            ss << " decimal_type: {"
                << " precision: " << ui32(parser.GetDecimal().Precision)
                << " scale: " << ui32(parser.GetDecimal().Scale)
                << "}; ";
            if (builder) {
                builder->AddNullableColumn(name, parser.GetDecimal());
            }
            break;
        default:
            Y_ENSURE(false, "Unexpected type kind# " << parser.GetKind() << " for column name# " << name.Quote());
    }
    return ss.Str();
}

NTable::TTableDescription TableDescriptionFromProto(const Ydb::Table::CreateTableRequest& proto) {
    NTable::TTableBuilder builder;

    for (const auto &col : proto.Getcolumns()) {
        LOG_DEBUG("AddNullableColumn: " << ProcessColumnType(col.Getname(), TType(col.Gettype()), &builder));
    }

    for (const auto &primary : proto.Getprimary_key()) {
       LOG_DEBUG("SetPrimaryKeyColumn: name: " << primary);
    }
    builder.SetPrimaryKeyColumns({proto.Getprimary_key().cbegin(), proto.Getprimary_key().cend()});

    return builder.Build();
}

NTable::TTableDescription TableDescriptionFromFile(const TString& filePath) {
    TFile file(filePath, OpenExisting | RdOnly);
    TString str = TString::Uninitialized(file.GetLength());
    file.Read(str.Detach(), file.GetLength());

    Ydb::Table::CreateTableRequest proto;
    google::protobuf::TextFormat::ParseFromString(str, &proto);
    return TableDescriptionFromProto(proto);
}

TString SerializeColumnsToString(const TVector<TColumn>& columns, TVector<TString> primary) {
    Sort(primary);
    TStringStream ss;
    for (const auto& col : columns) {
        ss << "  ";
        if (BinarySearch(primary.cbegin(), primary.cend(), col.Name)) {
            ss << "primary; ";
        }
        ss << ProcessColumnType(col.Name, col.Type, nullptr) << Endl;
    }
    // Cerr << "Parse column to : " << ss.Str() << Endl;
    return ss.Str();
}

void CheckTableDescriptionIsSame(const NTable::TTableDescription& backupDesc,
        const NTable::TTableDescription& realDesc) {
    if (backupDesc.GetColumns() != realDesc.GetColumns() ||
            backupDesc.GetPrimaryKeyColumns() != realDesc.GetPrimaryKeyColumns()) {
        LOG_ERR("Error");
        LOG_ERR("Table scheme from backup:");
        LOG_ERR(SerializeColumnsToString(backupDesc.GetColumns(), backupDesc.GetPrimaryKeyColumns()));
        LOG_ERR("Table scheme from database:");
        LOG_ERR(SerializeColumnsToString(realDesc.GetColumns(), realDesc.GetPrimaryKeyColumns()));
    } else {
        LOG_ERR("Ok");
    }
}

void UploadDataIntoTable(TDriver driver, const NTable::TTableDescription& tableDesc, const TString& relPath,
        const TString& absPath, TFsPath folderPath, const TRestoreFolderParams& params) {
    Y_ENSURE(!folderPath.Child(INCOMPLETE_DATA_FILE_NAME).Exists(),
            "There is incomplete data file in folder, path# " << TString(folderPath).Quote());
    ui32 fileCounter = 0;
    TFsPath dataFileName = folderPath.Child(CreateDataFileName(fileCounter++));

    if (params.UseBulkUpsert) {
        LOG_DEBUG("Going to BulkUpsert into table# " << absPath.Quote());
    }
    while (dataFileName.Exists()) {
        LOG_DEBUG("Going to read new data file, fileName# " << dataFileName);


        TUploader::TOptions opts;
        if (params.UploadBandwidthBPS) {
            opts.Rate = (opts.Interval.Seconds() * params.UploadBandwidthBPS + IO_BUFFER_SIZE - 1) / IO_BUFFER_SIZE;
            LOG_DEBUG("Custom bandwidth limit is specified, will use bandwidth# "
                << HumanReadableSize(params.UploadBandwidthBPS, SF_BYTES) << "B/s"
                << " RPS# " << double(opts.Rate) / opts.Interval.Seconds() << " reqs/s"
                << " IO buffer size# " << HumanReadableSize(IO_BUFFER_SIZE, SF_BYTES));
        }
        if (params.MaxUploadRps) {
            opts.Rate = params.MaxUploadRps * opts.Interval.Seconds();
        }
        opts.Rate = Max<ui64>(1, opts.Rate);

        TQueryFromFileIterator it(relPath, dataFileName, tableDesc.GetColumns(), IO_BUFFER_SIZE, params.MaxRowsPerQuery,
                params.MaxBytesPerQuery);
        NTable::TTableClient client(driver);
        TUploader uploader(opts, client, it.GetQueryString());
        if (!params.UseBulkUpsert) {
            LOG_DEBUG("Query string:\n" << it.GetQueryString());
        }

        while (!it.Empty()) {
            bool ok = false;
            if (params.UseBulkUpsert) {
                ok = uploader.Push(absPath, it.ReadNextGetValue());
            } else {
                ok = uploader.Push(it.ReadNextGetParams());
            }
            Y_ENSURE(ok, "Error in uploader.Push()");
        }
        uploader.WaitAllJobs();
        dataFileName = folderPath.Child(CreateDataFileName(fileCounter++));
    }
}

void RestoreTable(TDriver driver, const TString& database, const TString& prefix, TFsPath folderPath,
        const TRestoreFolderParams& params) {
    Y_ENSURE(!folderPath.Child(INCOMPLETE_FILE_NAME).Exists(),
            "There is incomplete file in folder, path# " << TString(folderPath).Quote());
    NTable::TTableClient client(driver);

    const TString relPath = JoinDatabasePath(prefix, folderPath.GetName());
    const TString absPath = JoinDatabasePath(database, relPath);
    LOG_DEBUG("Restore table from folder: " << folderPath << " in database path# " << absPath.Quote());

    NTable::TTableDescription tableDesc = TableDescriptionFromFile(folderPath.Child(SCHEME_FILE_NAME));


    if (params.OnlyCheck) {
        LOG_ERR("Check table: " << absPath.Quote() << "...");
        NTable::TTableDescription tableDescReal = DescribeTable(driver, absPath);
        CheckTableDescriptionIsSame(tableDesc, tableDescReal);
    } else {
        auto timer = GetVerbosity()
            ? MakeHolder<TScopedTimer>(TStringBuilder() << "Done restore table# " << absPath.Quote() << " took# ")
            : nullptr;
        // Create Table
        TStatus status = client.RetryOperationSync([absPath, &tableDesc](NTable::TSession session) {
            auto result = session.CreateTable(absPath, std::move(tableDesc)).GetValueSync();
            return result;
        });
        VerifyStatus(status, TStringBuilder() << "CreateTable on path: " << absPath.Quote());
        LOG_DEBUG("Table is created, path: " << absPath.Quote());
        if (!params.SchemaOnly) {
            UploadDataIntoTable(driver, tableDesc, relPath, absPath, folderPath, params);
        }
    }
}

void RestoreFolderImpl(TDriver driver, const TString& database, const TString& prefix, TFsPath folderPath,
        const TRestoreFolderParams& params) {
    LOG_DEBUG("Restore folder: " << folderPath);
    Y_ENSURE(folderPath, "folderPath cannot be empty on restore, please specify path to folder containing backup");
    Y_ENSURE(folderPath.IsDirectory(), "Specified folderPath " << folderPath.GetPath().Quote() << " must be a folder");
    Y_ENSURE(!folderPath.Child(INCOMPLETE_FILE_NAME).Exists(),
            "There is incomplete file in folder, path# " << TString(folderPath).Quote());

    if (prefix != "/" && !params.OnlyCheck) {
        LOG_DEBUG("Create prefix folder: " << prefix);
        NScheme::TSchemeClient client(driver);
        TString path = JoinDatabasePath(database, prefix);
        TStatus status = client.MakeDirectory(path).GetValueSync();
        VerifyStatus(status, TStringBuilder() << "MakeDirectory on path: " << path.Quote());
    }

    if (folderPath.Child(SCHEME_FILE_NAME).Exists()) {
        RestoreTable(driver, database, prefix, folderPath, params);
    } else {
        TVector<TFsPath> children;
        folderPath.List(children);
        for (const auto& child : children) {
            Y_ENSURE(folderPath.IsDirectory(), "Non directory and non table folder inside backup tree, "
                                               "path: " << child.GetPath().Quote());
            if (child.Child(SCHEME_FILE_NAME).Exists()) {
                RestoreTable(driver, database, prefix, child, params);
            } else {
                RestoreFolderImpl(driver, database, JoinDatabasePath(prefix, child.GetName()), child, params);
            }
        }
    }
}

static bool IsNamePresentedInDir(NScheme::TListDirectoryResult listResult, const TString& name) {
    for (const auto& child : listResult.GetChildren()) {
        if (child.Name == name) {
            return true;
        }
    }
    return false;
}

void CheckTablesAbsence(NScheme::TSchemeClient client, const TString& database, const TString& prefix, TFsPath folderPath) {
    Y_ENSURE(folderPath, "folderPath cannot be empty on restore, please specify path to folder containing backup");
    Y_ENSURE(folderPath.IsDirectory(), "Specified folderPath " << folderPath.GetPath().Quote() << " must be a folder");
    Y_ENSURE(!folderPath.Child(INCOMPLETE_FILE_NAME).Exists(),
            "There is incomplete file in folder, path# " << TString(folderPath).Quote());

    const TString path = JoinDatabasePath(database, prefix);
    TString name = folderPath.GetName();

    NScheme::TListDirectoryResult listResult = client.ListDirectory(path).GetValueSync();
    VerifyStatus(listResult, TStringBuilder() << "ListDirectory, path: " << path.Quote());

    const bool isTable = folderPath.Child(SCHEME_FILE_NAME).Exists();
    if (isTable) {
        Y_ENSURE(!IsNamePresentedInDir(listResult, name), "Table with name# " << name.Quote()
                << " is presented in path# " << path.Quote());
        LOG_DEBUG("\tOk! Table " << name.Quote() << " is absent in database path# " << path.Quote());
    } else {
        TVector<TFsPath> children;
        folderPath.List(children);
        for (const auto& child : children) {
            const bool isChildTable = child.Child(SCHEME_FILE_NAME).Exists();
            const TString childName = child.GetName();
            const bool isChildPresented = IsNamePresentedInDir(listResult, childName);
            if (isChildTable) {
                Y_ENSURE(!isChildPresented, "Table with name# " << childName.Quote()
                        << " is presented in path# " << path.Quote());
                LOG_DEBUG("\tOk! Table " << childName.Quote() << " is absent in database path# "
                    << path.Quote());
            } else {
                if (isChildPresented) {
                    LOG_DEBUG("\tOk! Directory " << childName.Quote() << " is presented in database path# "
                        << path.Quote() << ", so check tables in that dir");
                    CheckTablesAbsence(client, database, JoinDatabasePath(prefix, child.GetName()), child);
                } else {
                    LOG_DEBUG("\tOk! Directory " << childName.Quote() << " is absent in database path# "
                        << path.Quote());
                }
            }
        }
    }
}

void RestoreFolder(TDriver driver, const TString& database, const TString& prefix, const TFsPath folderPath,
        const TRestoreFolderParams& params) {
    NScheme::TSchemeClient client(driver);
    Y_ENSURE(prefix, "restore prefix cannot be empty, database# " << database.Quote() << " prefix# " << prefix.Quote());

    if (params.CheckTablesAbsence && !params.OnlyCheck) {
        LOG_DEBUG("Check absence of tables to be restored");
        if (prefix != "/") {
            TString path = JoinDatabasePath(database, prefix);
            TString parent = ParentPathFromDbPath(path);
            TString name = NameFromDbPath(path);
            LOG_DEBUG("Going to list parent# " << parent.Quote() << " for path path# " << path.Quote());
            NScheme::TListDirectoryResult listResult = client.ListDirectory(parent).GetValueSync();
            VerifyStatus(listResult, TStringBuilder() << "ListDirectory, path# " << parent.Quote());
            if (IsNamePresentedInDir(listResult, name)) {
                CheckTablesAbsence(client, database, prefix, folderPath);
            } else {
                LOG_DEBUG("\tOk! restore directory# " << path.Quote() << " is absent in database");
            }
        } else {
            CheckTablesAbsence(client, database, prefix, folderPath);
        }
        LOG_DEBUG("Check done, everything is Ok");
    }
    RestoreFolderImpl(driver, database, prefix, folderPath, params);
}

} //  NYdb::NBackup
