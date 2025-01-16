#pragma once

#include <ydb/core/base/path.h>

#include <ydb/library/accessor/accessor.h>

#include <util/str_stl.h>

namespace NKikimr::NColumnShard::NTiers {

class TExternalStorageId {
private:
    YDB_READONLY_DEF(TString, ConfigPath);

public:
    TExternalStorageId(const TString& path)
        : ConfigPath(CanonizePath(path)) {
    }

    TString ToString() const {
        return ConfigPath;
    }

    std::strong_ordering operator<=>(const TExternalStorageId& other) const {
        return ConfigPath <=> other.ConfigPath;
    }
    bool operator==(const TExternalStorageId& other) const {
        return ConfigPath == other.ConfigPath;
    }
};

}   // namespace NKikimr::NColumnShard::NTiers

template <>
struct THash<NKikimr::NColumnShard::NTiers::TExternalStorageId> {
    inline ui64 operator()(const NKikimr::NColumnShard::NTiers::TExternalStorageId& x) const noexcept {
        return THash<TString>()(x.ToString());
    }
};
