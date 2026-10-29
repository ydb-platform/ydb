#include "sys.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::NConsoleClient {

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

bool IsTemporaryDir(const NScheme::TSchemeEntry& entry) {
    return entry.Type == NScheme::ESchemeEntryType::Directory && IsTemporaryName(entry.Name);
}

bool IsInternalDir(const NScheme::TSchemeEntry& entry) {
    return entry.Type == NScheme::ESchemeEntryType::Directory && IsInternalName(entry.Name);
}

bool IsInternalObject(const NScheme::TSchemeEntry& entry) {
    return IsSystemObject(entry) || IsTemporaryDir(entry);
}

}
