#pragma once

#include "defs.h"

#include <ydb/core/protos/config.pb.h>

namespace NKikimr {

class TFeatureFlags: public NKikimrConfig::TFeatureFlags {
    using TBase = NKikimrConfig::TFeatureFlags;

public:
    using TBase::TBase;
    using TBase::operator=;

    inline std::optional<bool> GetEnableMvcc() const {
        switch (TBase::GetEnableMvcc()) {
        case NKikimrConfig::TFeatureFlags::UNSET:
            return std::nullopt;
        case NKikimrConfig::TFeatureFlags::VALUE_TRUE:
            return true;
        case NKikimrConfig::TFeatureFlags::VALUE_FALSE:
            return false;
        }
    }

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
