#include "execution_context.h"

namespace NKvVolumeStress {

TExecutionContext::TExecutionContext(
    ui64 executionId,
    ui32 actionId,
    TString actionName,
    TPtr parent,
    TKeyBucket* workerStorage)
    : ExecutionId(executionId)
    , ActionId(actionId)
    , ActionName(std::move(actionName))
    , Parent(std::move(parent))
    , WorkerStorage_(workerStorage)
{
}

TExecutionContext::~TExecutionContext() {
    if (!WorkerStorage_) {
        return;
    }

    const TVector<std::pair<TString, TKeyInfo>> remaining = Keys_.Drain();
    if (!remaining.empty()) {
        WorkerStorage_->AddKeys(remaining);
    }
}

void TExecutionContext::AddKey(const TString& key, const TKeyInfo& keyInfo) {
    Keys_.AddKey(key, keyInfo);
}

void TExecutionContext::AddKeys(const TVector<std::pair<TString, TKeyInfo>>& keys) {
    Keys_.AddKeys(keys);
}

TVector<std::pair<TString, TKeyInfo>> TExecutionContext::PickKeys(ui32 count, bool erase) {
    return Keys_.PickKeys(count, erase);
}

} // namespace NKvVolumeStress
