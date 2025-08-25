#include "sys.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::NConsoleClient {

bool IsSystemDir(const NScheme::TSchemeEntry& entry) {
    if (entry.Type == NScheme::ESchemeEntryType::Directory) {
        return entry.Name.starts_with("~")
            || entry.Name.starts_with(".sys")
            || entry.Name.starts_with(".metadata");
    } else {
        return false;
    }
}

bool IsSystemObject(const NScheme::TSchemeEntry& entry) {
    return IsSystemDir(entry) || entry.Type == NScheme::ESchemeEntryType::SysView;
}

}
