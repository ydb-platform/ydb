#pragma once

#include "reader.h"

#include <functional>

namespace NIPREG {

void MergeIPREGS(TReader &a, TReader& b, std::function<void(const TAddress& first, const TAddress& last, const TString *a, const TString *b)>&& proc);

}
