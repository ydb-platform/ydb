#include "sys.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <util/generic/hash_set.h>

namespace NYdb::NConsoleClient {

namespace {

// In future should be syncronized with 'ydb/core/tx/schemeshard/schemeshard_system_names.cpp'
const THashSet<TStringBuf> SystemReservedNames = {
    SYSVIEWS_DIR_NAME,
    METADATA_DIR_NAME,
    TMP_DIR_NAME,
    BACKUPS_DIR_NAME,
};

}

bool IsSystemName(const TStringBuf name) {
    return SystemReservedNames.contains(name);
}

bool IsSystemDir(const NScheme::TSchemeEntry& entry) {
    if (entry.Type == NScheme::ESchemeEntryType::Directory) {
        return IsSystemName(entry.Name);
    } else {
        return false;
    }
}

bool IsSystemObject(const NScheme::TSchemeEntry& entry) {
    return IsSystemDir(entry) || entry.Type == NScheme::ESchemeEntryType::SysView;
}

bool IsBackupName(const TStringBuf name) {
    return name.StartsWith(TEMPORARY_BACKUP_DIR_PREFIX);
}

bool IsBackupDir(const NScheme::TSchemeEntry& entry) {
    return entry.Type == NScheme::ESchemeEntryType::Directory && IsBackupName(entry.Name);
}

bool IsInternalName(const TStringBuf name) {
    return IsSystemName(name) || IsBackupName(name);
}

bool IsInternalDir(const NScheme::TSchemeEntry& entry) {
    return IsSystemDir(entry) || IsBackupDir(entry);
}

bool IsInternalObject(const NScheme::TSchemeEntry& entry) {
    return IsSystemObject(entry) || IsBackupDir(entry);
}

}
