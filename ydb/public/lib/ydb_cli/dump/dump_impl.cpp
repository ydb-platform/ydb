#include "dump_impl.h"

#include <ydb/library/backup/backup.h>
#include <ydb/library/backup/util.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>

namespace NYdb {
namespace NDump {

TDumpClient::TDumpClient(const TDriver& driver, const std::shared_ptr<TLog>& log)
    : Driver(driver)
    , Log(log)
{
}

TDumpResult TDumpClient::Dump(const TString& dbPath, const TString& fsPath, const TDumpSettings& settings) {
    try {
        NBackup::SetLog(Log);
        NBackup::BackupFolder(
            Driver,
            settings.Database_,
            RelPathFromAbsolute(settings.Database_, dbPath),
            fsPath,
            settings.ExclusionPatterns_,
            settings.SchemaOnly_,
            settings.UseConsistentCopyTable(),
            settings.AvoidCopy_,
            settings.SavePartialResult_,
            settings.PreservePoolKinds_,
            settings.Ordered_);
        return Result<TDumpResult>();
    } catch (NBackup::TYdbErrorException& e) {
        return TDumpResult(std::move(e.Status));
    } catch (const yexception& e) {
        return Result<TDumpResult>(EStatus::INTERNAL_ERROR, e.what());
    }
}

} // NDump
} // NYdb
