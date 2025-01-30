#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartDataErasure(const TString& /*tenant*/) {
    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnDataErasureTimeout(const TString& /*tenant*/) {}

} // NKikimr::NSchemeShard
