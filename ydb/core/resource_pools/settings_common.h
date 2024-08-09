#pragma once

#include <util/string/cast.h>


namespace NKikimr::NResourcePool {

struct TSettingsBase {
    struct TParser {
        const TString& Value;
    };

    struct TExtractor {
    };

    bool operator==(const TSettingsBase& other) const = default;
};

}  // namespace NKikimr::NResourcePool
