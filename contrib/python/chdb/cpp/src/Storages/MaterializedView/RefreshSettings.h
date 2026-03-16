#pragma once

#include <Core/BaseSettings.h>

namespace DB_CHDB
{

#define LIST_OF_REFRESH_SETTINGS(M, ALIAS) \
    /// TODO: Add settings
    ///       M(UInt64, name, 42, "...", 0)

DECLARE_SETTINGS_TRAITS(RefreshSettingsTraits, LIST_OF_REFRESH_SETTINGS)

struct RefreshSettings : public BaseSettings<RefreshSettingsTraits> {};

}
