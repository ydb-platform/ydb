#pragma once

#include "dump.h"

#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/folder/path.h>

namespace NYdb {
namespace NDump {

extern const char DOC_API_TABLE_VERSION_ATTR[23];
extern const char DOC_API_REQUEST_TYPE[22];

namespace NPrivate {

class IDataAccumulator {
public:
    virtual ~IDataAccumulator() = default;
    virtual bool Fits(const TString& line) const = 0;
    virtual void Feed(TString&& line) = 0;
    virtual bool Ready(bool force = false) const = 0;
    virtual TString GetData(bool force = false) = 0;
};

class IDataWriter {
public:
    virtual ~IDataWriter() = default;
    virtual bool Push(TString&& data) = 0;
    virtual void Wait() = 0;
};

} // NPrivate

class TRestoreClient {
    TRestoreResult RestoreFolder(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);
    TRestoreResult RestoreTable(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);

    TRestoreResult CheckSchema(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestoreData(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const NTable::TTableDescription& desc);
    TRestoreResult RestoreIndexes(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestorePermissions(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const THashSet<TString>& oldEntries);

public:
    explicit TRestoreClient(
        NImport::TImportClient& importClient,
        NOperation::TOperationClient& operationClient,
        NScheme::TSchemeClient& SchemeClient,
        NTable::TTableClient& tableClient);

    TRestoreResult Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings = {});

private:
    NImport::TImportClient& ImportClient;
    NOperation::TOperationClient& OperationClient;
    NScheme::TSchemeClient& SchemeClient;
    NTable::TTableClient& TableClient;

}; // TRestoreClient

} // NDump
} // NYdb
