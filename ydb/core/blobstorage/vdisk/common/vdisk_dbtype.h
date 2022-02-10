#pragma once

#include "defs.h"

namespace NKikimr {

    ///////////////////////////////////////////////////////////////////////////////////////
    // EHullDbType
    ///////////////////////////////////////////////////////////////////////////////////////
    enum class EHullDbType : ui32 {
        First = 0,
        LogoBlobs = 0,
        Blocks = 1,
        Barriers = 2,
        Max = 3,
    };

    static inline const char* EHullDbTypeToString(EHullDbType type) {
        switch (type) {
            case EHullDbType::LogoBlobs:    return "LogoBlobs";
            case EHullDbType::Blocks:       return "Blocks";
            case EHullDbType::Barriers:     return "Barriers";
            case EHullDbType::Max:          return "Max";
        }
    }

    static inline EHullDbType StringToEHullDbType(const TString &str) {
        if (str == "LogoBlobs") {
            return EHullDbType::LogoBlobs;
        } else if (str == "Blocks") {
            return EHullDbType::Blocks;
        } else if (str == "Barriers") {
            return EHullDbType::Barriers;
        } else {
            return EHullDbType::Max;
        }
    }

    constexpr inline ui32 Mask(EHullDbType t) {
        return 1u << ui32(t);
    }

    static const ui32 AllEHullDbTypes =
        Mask(EHullDbType::LogoBlobs) |
        Mask(EHullDbType::Blocks) |
        Mask(EHullDbType::Barriers);

    template <class TKey>
    EHullDbType TKeyToEHullDbType();

} // NKikimr

