#pragma once

namespace NYdb {

namespace NScheme {
    struct TSchemeEntry;
}

namespace NConsoleClient {

bool IsSystemObject(const NScheme::TSchemeEntry& entry);

} // NConsoleClient
} // NYdb
