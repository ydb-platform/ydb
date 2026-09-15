#pragma once

#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NYdb::NConsoleClient {

// Joins path parts with '/' separator
TString JoinYdbPath(const TVector<TString>& path);

// Canonizes path: ensures single leading '/', removes trailing '/', collapses multiple slashes
TString CanonizeYdbPath(const TString& path);

}

