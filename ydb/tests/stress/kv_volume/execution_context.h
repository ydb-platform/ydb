#pragma once

#include "key_bucket.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <memory>

namespace NKvVolumeStress {

class TExecutionContext {
public:
    using TPtr = std::shared_ptr<TExecutionContext>;

    TExecutionContext(
        ui64 executionId,
        ui32 actionId,
        TString actionName,
        TPtr parent,
        TKeyBucket* workerStorage);
    ~TExecutionContext();

    void AddKey(const TString& key, const TKeyInfo& keyInfo);
    void AddKeys(const TVector<std::pair<TString, TKeyInfo>>& keys);
    TVector<std::pair<TString, TKeyInfo>> PickKeys(ui32 count, bool erase);

    const ui64 ExecutionId = 0;
    const ui32 ActionId = 0;
    const TString ActionName;
    const TPtr Parent;

private:
    TKeyBucket* const WorkerStorage_ = nullptr;
    TKeyBucket Keys_;
};

} // namespace NKvVolumeStress
