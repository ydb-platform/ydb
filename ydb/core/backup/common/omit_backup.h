#pragma once

#include <util/generic/strbuf.h>

namespace Ydb::Table {
class ChangefeedDescription;
}

namespace NKikimr::NBackup {

// Objects carrying this attribute are omitted from backup and export.
// Directory-like objects are not supported: this is an object-local flag, not
// an inherited subtree policy. Removing it clears only this exclusion rule.
constexpr TStringBuf OMIT_BACKUP_ATTRIBUTE = "__omit_backup";
constexpr TStringBuf OMIT_BACKUP_VALUE = "true";

template <typename TAttributes>
bool IsOmittedFromBackup(const TAttributes& attributes) {
    const auto it = attributes.find(typename TAttributes::key_type(OMIT_BACKUP_ATTRIBUTE));
    return it != attributes.end() && it->second == OMIT_BACKUP_VALUE;
}

constexpr TStringBuf ASYNC_REPLICATION_ATTRIBUTE = "__async_replication";

bool IsKeyRangeReplicationBackup(const Ydb::Table::ChangefeedDescription& changefeed);
bool IsChangefeedOmittedFromBackup(const Ydb::Table::ChangefeedDescription& changefeed, bool allowKeyRange);

} // namespace NKikimr::NBackup
