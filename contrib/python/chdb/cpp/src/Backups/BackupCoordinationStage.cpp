#include <Backups/BackupCoordinationStage.h>
#include <fmt/format.h>


namespace DB_CHDB
{

String BackupCoordinationStage::formatGatheringMetadata(int attempt_no)
{
    return fmt::format("{} ({})", GATHERING_METADATA, attempt_no);
}

}
