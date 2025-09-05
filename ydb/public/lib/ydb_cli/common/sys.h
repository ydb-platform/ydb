#pragma once

#include <util/generic/fwd.h>
namespace NYdb {

inline namespace Dev {
namespace NScheme {
    struct TSchemeEntry;
}
}

namespace NConsoleClient {

bool IsSystemName(const TStringBuf name);

bool IsSystemDir(const NScheme::TSchemeEntry& entry);

bool IsSystemObject(const NScheme::TSchemeEntry& entry);

} // NConsoleClient
} // NYdb
