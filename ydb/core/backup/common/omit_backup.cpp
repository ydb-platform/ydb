#include "omit_backup.h"

#include <ydb/core/tx/replication/common/changefeed.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <string>

namespace NKikimr::NBackup {

bool IsKeyRangeReplicationBackup(const Ydb::Table::ChangefeedDescription& changefeed) {
    const auto& attributes = changefeed.attributes();
    const auto it = attributes.find(std::string(ASYNC_REPLICATION_ATTRIBUTE));
    return it != attributes.end()
        && changefeed.resolved_timestamps_interval().seconds() > 0
        && NReplication::HasChangefeedCapability(it->second, NReplication::KeyRangeCapability);
}

bool IsChangefeedOmittedFromBackup(const Ydb::Table::ChangefeedDescription& changefeed, bool allowKeyRange) {
    return IsOmittedFromBackup(changefeed.attributes())
        || (changefeed.attributes().count(std::string(ASYNC_REPLICATION_ATTRIBUTE))
            && !(allowKeyRange && IsKeyRangeReplicationBackup(changefeed)));
}

} // namespace NKikimr::NBackup
