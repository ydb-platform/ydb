#include "sys.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <util/generic/hash_set.h>

namespace NYdb::NConsoleClient {

namespace {

// In future should be syncronized with 'ydb/core/tx/schemeshard/schemeshard_system_names.cpp'
const THashSet<TStringBuf> SystemReservedNames = {
    ".sys",
    ".metadata",
    ".tmp",
    ".backups",
};

}

bool IsSystemName(const TStringBuf name) {
    return SystemReservedNames.contains(name)
        || name.StartsWith("~");
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

}
