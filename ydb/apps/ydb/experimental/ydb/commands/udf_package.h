#pragma once

#include <util/generic/string.h>

#include <string>

namespace NYdb::NConsoleClient {

struct TUdfPackage {
    std::string Manifest;
    std::string Body;
};

TUdfPackage ParseUdfPackage(TStringBuf data);
TUdfPackage ReadUdfPackage(const TString& path);

} // namespace NYdb::NConsoleClient
