#pragma once

#include "runtime_settings.h"

#include <yql/essentials/utils/strong_alias.h>

#include <util/generic/strbuf.h>
#include <util/str_stl.h>
#include <util/system/types.h>

#include <array>

namespace NYql {

using TRuntimeSettingsStableHash = TStrongAlias<class TRuntimeSettingsStableHashTag, std::array<ui8, 32>>;

TRuntimeSettingsStableHash StableHashRuntimeSettings(const TRuntimeSettings& config);

IOutputStream& operator<<(IOutputStream& out, const TRuntimeSettingsStableHash& hash);

} // namespace NYql

template <>
struct THash<NYql::TRuntimeSettingsStableHash> {
    size_t operator()(const NYql::TRuntimeSettingsStableHash& hash) const {
        return THash<TStringBuf>{}(TStringBuf(reinterpret_cast<const char*>(hash->data()), hash->size()));
    }
};
