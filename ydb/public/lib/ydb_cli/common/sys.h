#pragma once

namespace NYdb {

inline namespace Dev {
namespace NScheme {
    struct TSchemeEntry;
}
}

namespace NConsoleClient {

bool IsSystemDir(const NScheme::TSchemeEntry& entry);

bool IsSystemObject(const NScheme::TSchemeEntry& entry);

} // NConsoleClient
} // NYdb
