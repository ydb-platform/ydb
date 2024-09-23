#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <library/cpp/regex/pcre/regexp.h>

#include <util/folder/path.h>
#include <util/generic/singleton.h>
#include <util/stream/output.h>
#include <util/system/file.h>

namespace NYdb {
namespace NBackup {

class TYdbErrorException : public yexception {
public:
    TStatus Status;

    TYdbErrorException(const TStatus& status)
        : Status(status) {}

    void LogToStderr() const;
};

void BackupFolder(TDriver driver, const TString& database, const TString& relDbPath, TFsPath folderPath,
        const TVector<TRegExMatch>& exclusionPatterns,
        bool schemaOnly, bool useConsistentCopyTable, bool avoidCopy = false, bool savePartialResult = false,
        bool preservePoolKinds = false, bool ordered = false);

struct TRestoreFolderParams {
    bool OnlyCheck = false;
    bool SchemaOnly = false;
    bool CheckTablesAbsence = true;
    ////////////////////////////////////////
    // Only one parameters set can be used. Either
    ui64 UploadBandwidthBPS = 0;
    // or
    ui64 MaxRowsPerQuery = 0;
    ui64 MaxBytesPerQuery = 0;
    ui64 MaxUploadRps = 0;
    ////////////////////////////////////////
    bool UseBulkUpsert = false;

    bool CheckRps() const {
        bool oldBPSLimit = UploadBandwidthBPS > 0;
        bool newRpsLimit = MaxRowsPerQuery > 0 || MaxBytesPerQuery > 0 || MaxUploadRps > 0;
        return !oldBPSLimit || !newRpsLimit;
    }
};

void RestoreFolder(TDriver driver, const TString& database, const TString& prefix, const TFsPath folderPath,
        const TRestoreFolderParams& params);

// For unit-tests only
TMaybe<TValue> ProcessResultSet(TStringStream& ss, TResultSetParser resultSetParser,
        TFile* dataFile = nullptr, const NTable::TTableDescription* desc = nullptr);
void PrintValue(IOutputStream& out, TValueParser& parser);

} // NBackup
} // NYdb
