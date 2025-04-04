#pragma once

#include "run_params.h"
#include "printout.h"

namespace NKikimr {
namespace NMiniKQL {

template<bool LLVM, bool Spilling>
void RunTestCombineLastSimple(const TRunParams& params, TTestResultCollector& printout);

}
}
