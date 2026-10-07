#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

namespace NKikimr::NPDiskTool {

void PrintUsage(const TString& argv0);

int RunCommand(const TString& command, int argc, char** argv);

} // namespace NKikimr::NPDiskTool
