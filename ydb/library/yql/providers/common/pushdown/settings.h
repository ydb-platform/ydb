#pragma once

#include <util/system/types.h>

namespace NYql::NPushdown {

struct TSettings {
    enum EFeatureFlag : ui64 {
        LikeOperator = 1,
        LikeOperatorOnlyForUtf8 = 1 << 1,
        JsonQueryOperators = 1 << 2,
        JsonExistsOperator = 1 << 3,
    };

    TSettings() = default;
    TSettings(const TSettings&) = default;

    void Enable(ui64 flagsMask, bool set = true);

    bool IsEnabled(EFeatureFlag flagMask) const;

private:
    ui64 FeatureFlags = 0;
};

} // namespace NYql::NPushdown
