#pragma once

#include <ydb/core/base/path.h>

#include <ydb/library/accessor/accessor.h>

#include <util/str_stl.h>

namespace NKikimr::NColumnShard::NTiers {

class TExternalStorageId {
private:
    YDB_READONLY_DEF(TString, Path);

public:
    TExternalStorageId(const TString& path)
        : Path(CanonizePath(path)) {
    }

    bool operator<(const TExternalStorageId& other) const {
        return Path < other.Path;
    }
    bool operator>(const TExternalStorageId& other) const {
        return Path > other.Path;
    }
    bool operator==(const TExternalStorageId& other) const {
        return Path == other.Path;
    }
};

}   // namespace NKikimr::NColumnShard::NTiers

template <>
struct THash<NKikimr::NColumnShard::NTiers::TExternalStorageId> {
    inline ui64 operator()(const NKikimr::NColumnShard::NTiers::TExternalStorageId& x) const noexcept {
        return THash<TString>()(x.GetPath());
    }
};
