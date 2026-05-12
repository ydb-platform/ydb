#pragma once

#include "types.h"

namespace NMVP::NSupportLinks {

TString BuildResponseBody(const TVector<TResolveOutput>* sourceOutputs, const TVector<TSupportError>& pendingErrors);

} // namespace NMVP::NSupportLinks
