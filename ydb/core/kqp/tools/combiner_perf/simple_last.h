#pragma once

#include "run_params.h"

namespace NKikimr {
namespace NMiniKQL {

template<bool LLVM, bool Spilling>
void RunTestLastSimple(const TRunParams& params);

}
}
