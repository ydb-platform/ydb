#pragma once
#include "factories.h"
#include <memory>

// function sets up termination handler, call it first in main() function
void SetupTerminateHandler();
// YDB Main function parameterized by TModuleFactories
int ParameterizedMain(int argc, char **argv, std::shared_ptr<NKikimr::TModuleFactories> factories);

