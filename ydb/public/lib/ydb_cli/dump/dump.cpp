#include "dump.h"
#include "dump_impl.h"
#include "restore_impl.h"

#include <ydb/public/sdk/cpp/client/ydb_import/import.h>
#include <ydb/public/sdk/cpp/client/ydb_operation/operation.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/logger/log.h>

#include <util/string/printf.h>

namespace NYdb {
namespace NDump {

extern const char SCHEME_FILE_NAME[] = "scheme.pb";
extern const char PERMISSIONS_FILE_NAME[] = "permissions.pb";
extern const char INCOMPLETE_FILE_NAME[] = "incomplete";
extern const char EMPTY_FILE_NAME[] = "empty_dir";

TString DataFileName(ui32 id) {
    return Sprintf("data_%02d.csv", id);
}

class TClient::TImpl {
public:
    explicit TImpl(const TDriver& driver, std::shared_ptr<TLog>&& log)
        : Log(log)
        , ImportClient(driver)
        , OperationClient(driver)
        , SchemeClient(driver)
        , TableClient(driver)
    {
    }

    TDumpResult Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings) {
        auto client = TDumpClient(SchemeClient, TableClient);
        return client.Dump(dbPath, fsPath, settings);
    }

    TRestoreResult Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings) {
        auto client = TRestoreClient(*Log, ImportClient, OperationClient, SchemeClient, TableClient);
        return client.Restore(fsPath, dbPath, settings);
    }

private:
    std::shared_ptr<TLog> Log;
    NImport::TImportClient ImportClient;
    NOperation::TOperationClient OperationClient;
    NScheme::TSchemeClient SchemeClient;
    NTable::TTableClient TableClient;

}; // TImpl

TDumpResult::TDumpResult(TStatus&& status)
    : TStatus(std::move(status))
{
}

TRestoreResult::TRestoreResult(TStatus&& status)
    : TStatus(std::move(status))
{
}

TClient::TClient(const TDriver& driver, std::shared_ptr<TLog>&& log)
    : Impl_(new TImpl(driver, std::move(log)))
{
}

TDumpResult TClient::Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings) {
    return Impl_->Dump(dbPath, fsPath, settings);
}

TRestoreResult TClient::Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings) {
    return Impl_->Restore(fsPath, dbPath, settings);
}

} // NDump
} // NYdb
