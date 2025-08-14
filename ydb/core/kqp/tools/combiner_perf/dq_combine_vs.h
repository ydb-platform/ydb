#pragma once

#include "run_params.h"
#include "printout.h"

namespace NKikimr {
namespace NMiniKQL {

template<bool LLVM>
void RunTestDqHashCombineVsWideCombine(const TRunParams& params, TTestResultCollector& printout);

}
}
