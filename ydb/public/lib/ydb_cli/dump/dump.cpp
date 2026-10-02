#include "dump.h"
#include "dump_impl.h"
#include "restore_impl.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <library/cpp/logger/log.h>

#include <util/string/printf.h>

namespace NYdb::NDump {

TString DataFileName(ui32 id) {
    return Sprintf("data_%02d.csv", id);
}

class TClient::TImpl {
public:
    explicit TImpl(const TDriver& driver, std::shared_ptr<TLog>&& log)
        : Driver(driver)
        , Log(std::move(log))
    {
    }

    TDumpResult Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings) {
        auto client = TDumpClient(Driver, Log);
        return client.Dump(dbPath, fsPath, settings);
    }

    TRestoreResult Restore(const TString& fsPath, const TString& dbPath, const TRestoreSettings& settings) {
        auto client = TRestoreClient(Driver, Log);
        return client.Restore(fsPath, dbPath, settings);
    }

    TDumpResult DumpCluster(const TString& fsPath) {
        auto client = TDumpClient(Driver, Log);
        return client.DumpCluster(fsPath);
    }

    TRestoreResult RestoreCluster(const TString& fsPath, const TRestoreClusterSettings& settings) {
        auto client = TRestoreClient(Driver, Log);
        return client.RestoreCluster(fsPath, settings);
    }

    TDumpResult DumpDatabase(const TString& database, const TString& fsPath) {
        auto client = TDumpClient(Driver, Log);
        return client.DumpDatabase(database, fsPath);
    }

    TRestoreResult RestoreDatabase(const TString& fsPath, const TRestoreDatabaseSettings& settings) {
        auto client = TRestoreClient(Driver, Log);
        return client.RestoreDatabase(fsPath, settings);
    }

private:
    const TDriver Driver;
    std::shared_ptr<TLog> Log;

}; // TImpl

TDumpResult::TDumpResult(TStatus&& status)
    : TStatus(std::move(status))
{
}

TRestoreResult::TRestoreResult(TStatus&& status)
    : TStatus(std::move(status))
{
}

TClient::TClient(const TDriver& driver)
    : Impl_(new TImpl(driver, std::make_shared<TLog>(CreateLogBackend("cerr"))))
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

TDumpResult TClient::DumpCluster(const TString& fsPath) {
    return Impl_->DumpCluster(fsPath);
}

TRestoreResult TClient::RestoreCluster(const TString& fsPath, const TRestoreClusterSettings& settings) {
     return Impl_->RestoreCluster(fsPath, settings);
}

TDumpResult TClient::DumpDatabase(const TString& database, const TString& fsPath) {
    return Impl_->DumpDatabase(database, fsPath);
}

TRestoreResult TClient::RestoreDatabase(const TString& fsPath, const TRestoreDatabaseSettings& settings) {
    return Impl_->RestoreDatabase(fsPath, settings);
}

} // NYdb::NDump
