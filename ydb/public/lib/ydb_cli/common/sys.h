#pragma once

namespace NYdb {

inline namespace Dev {
namespace NScheme {
    struct TSchemeEntry;
}
}

namespace NConsoleClient {

bool IsSystemObject(const NScheme::TSchemeEntry& entry);

} // NConsoleClient
} // NYdb
