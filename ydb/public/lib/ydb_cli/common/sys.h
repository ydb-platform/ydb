#pragma once

namespace NYdb {

inline namespace V3 {
namespace NScheme {
    struct TSchemeEntry;
}
}

namespace NConsoleClient {

bool IsSystemObject(const NScheme::TSchemeEntry& entry);

} // NConsoleClient
} // NYdb
