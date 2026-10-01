#pragma once

#include "printout.h"
#include "run_params.h"

namespace NKikimr::NMiniKQL {

template<bool LLVM, bool Spilling>
void RunTestDqBlock(TRunParams params, TTestResultCollector& printout);

}
