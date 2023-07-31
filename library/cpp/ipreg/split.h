#pragma once

#include "reader.h"

#include <util/generic/vector.h>

#include <functional>

namespace NIPREG {

void SplitIPREG(TReader &reader, std::function<void(const TAddress& first, const TAddress& last, const TVector<TString>& data)>&& proc);

}
