#pragma once

#include "run_params.h"
#include "printout.h"

namespace NKikimr {
namespace NMiniKQL {


void RunJoinsBench(const TRunParams& params, TTestResultCollector& printout);

}
}