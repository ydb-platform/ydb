#pragma once

#include "command.h"

namespace NYdb {
namespace NConsoleClient {
    TString NormalizePath(const TString &path);
    void AdjustPath(TString& path, const TClientCommand::TConfig& config);
    // Also resolves CLI references to the database itself, such as '.'.
    void AdjustPathToDatabase(TString& path, TClientCommand::TConfig& config);
}
}
