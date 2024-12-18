#pragma once

#include "dump.h"

#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
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
    TRestoreResult RestoreFolder(const TFsPath& fsPath, const TString& dbRestoreRoot, const TString& dbPathRelativeToRestoreRoot, const TRestoreSettings& settings);
    TRestoreResult RestoreTable(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings);
    TRestoreResult RestoreView(const TFsPath& fsPath, const TString& dbRestoreRoot, const TString& dbPathRelativeToRestoreRoot, const TRestoreSettings& settings);

    TRestoreResult CheckSchema(const TString& dbPath, const NTable::TTableDescription& desc);
    TRestoreResult RestoreData(const TFsPath& fsPath, const TString& dbPath, const TRestoreSettings& settings, const NTable::TTableDescription& desc);
    TRestoreResult RestoreIndexes(const TString& dbPath, const NTable::TTableDescription& desc);

public:
    explicit TRestoreClient(
        NImport::TImportClient& importClient,
        NOperation::TOperationClient& operationClient,
        NScheme::TSchemeClient& SchemeClient,
        NTable::TTableClient& tableClient,
        NQuery::TQueryClient& queryClient);

    TRestoreResult Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings = {});

private:
    NImport::TImportClient& ImportClient;
    NOperation::TOperationClient& OperationClient;
    NScheme::TSchemeClient& SchemeClient;
    NTable::TTableClient& TableClient;
    NQuery::TQueryClient& QueryClient;

    struct TRestoreViewCall {
        TFsPath FsPath;
        TString DbRestoreRoot;
        TString DbPathRelativeToRestoreRoot;
        TRestoreSettings Settings;
        bool IsAlreadyExisting;
    };
    // Views usually depend on other objects.
    // If the dependency is not created yet, then the view restoration will fail.
    // We retry failed view creation attempts until either all views are created, or the errors are persistent.
    TVector<TRestoreViewCall> ViewRestorationCalls;

}; // TRestoreClient

} // NDump
} // NYdb
