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
    friend IOutputStream& operator<<(IOutputStream& out, const TExternalStorageId& storageId) {
        return out << storageId.ToString();
    }

    std::strong_ordering operator<=>(const TExternalStorageId& other) const {
        if (ConfigPath < other.ConfigPath) {
            return std::strong_ordering::less;
        }
        if (ConfigPath > other.ConfigPath) {
            return std::strong_ordering::greater;
        }
        return std::strong_ordering::equal;
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
