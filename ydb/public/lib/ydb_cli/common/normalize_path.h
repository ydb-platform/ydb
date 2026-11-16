#pragma once

#include "command.h"

namespace NYdb {
namespace NConsoleClient {
    TString NormalizePath(const TString &path);
    void AdjustPath(TString& path, const TClientCommand::TConfig& config);
}
}
