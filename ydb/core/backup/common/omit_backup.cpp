#include "omit_backup.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <string>

namespace NKikimr::NBackup {

bool IsChangefeedOmittedFromBackup(const Ydb::Table::ChangefeedDescription& changefeed) {
    return IsOmittedFromBackup(changefeed.attributes())
        || changefeed.attributes().count(std::string(ASYNC_REPLICATION_ATTRIBUTE)) > 0;
}

} // namespace NKikimr::NBackup
