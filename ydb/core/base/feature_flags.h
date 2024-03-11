#pragma once

#include "defs.h"

#include <ydb/core/protos/feature_flags.pb.h>

namespace NKikimr {

class TFeatureFlags: public NKikimrConfig::TFeatureFlags {
    using TBase = NKikimrConfig::TFeatureFlags;

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
