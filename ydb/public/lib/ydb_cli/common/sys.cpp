#include "sys.h"

#include <ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::NConsoleClient {

bool IsSystemObject(const NScheme::TSchemeEntry& entry) {
    if (entry.Type != NScheme::ESchemeEntryType::Directory) {
        return false;
    }

    return entry.Name.StartsWith("~")
        || entry.Name.StartsWith(".sys")
        || entry.Name.StartsWith(".metadata");
}

}
