#pragma once
#include "factories.h"
#include <memory>

// function sets up termination handler, call it first in main() function
void SetupTerminateHandler();
// YDB Main function parameterized by TModuleFactories
int ParameterizedMain(int argc, char **argv, std::shared_ptr<NKikimr::TModuleFactories> factories);

namespace NKikimr {

struct TKikimrRunConfig;

int MainRun(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories);
void PrintAllocatorInfoAndExit();
void PrintCompatibilityInfoAndExit();

} // namespace NKikimr

