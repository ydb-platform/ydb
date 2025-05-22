#pragma once
#include "defs.h"
#include "runtime_feature_flags.h"

namespace NKikimr {

/**
 * Thin wrapper around TRuntimeFeatureFlags, for compatibility with existing code
 */
class TFeatureFlags : public TRuntimeFeatureFlags {
    using TBase = TRuntimeFeatureFlags;

public:
    using TBase::TBase;
    using TBase::operator=;

    inline void SetEnableBackgroundCompactionForTest(bool value) {
        SetEnableBackgroundCompaction(value);
    }

    inline void SetEnableBackgroundCompactionServerlessForTest(bool value) {
        SetEnableBackgroundCompactionServerless(value);
    }

    inline void SetEnableBorrowedSplitCompactionForTest(bool value) {
        SetEnableBorrowedSplitCompaction(value);
    }
};

} // NKikimr
