#pragma once

#include <ydb/core/base/path.h>

#include <ydb/library/accessor/accessor.h>

#include <util/str_stl.h>
#include <util/string/cast.h>

#include <optional>
#include <tuple>

namespace NKikimr::NColumnShard::NTiers {

class TExternalStorageId {
private:
    YDB_READONLY_DEF(TString, ConfigPath);
    YDB_READONLY_DEF(std::optional<TString>, ObjectKeyPrefix);

public:
    TExternalStorageId(const TString& configPath, const std::optional<TString>& objectKeyPrefix = std::nullopt)
        : ConfigPath(CanonizePath(configPath))
        , ObjectKeyPrefix(objectKeyPrefix)
    {
    }

    static TExternalStorageId FromString(const TString& serialized) {
        if (!serialized.StartsWith("s3:")) {
            return TExternalStorageId(serialized);
        }

        TStringBuf value(serialized);
        value.Skip(3);
        TStringBuf length;
        Y_ABORT_UNLESS(value.TrySplit(':', length, value));
        const size_t pathSize = ::FromString<size_t>(length);
        Y_ABORT_UNLESS(pathSize > 0 && pathSize <= value.size());
        return TExternalStorageId(TString(value.SubStr(0, pathSize)), TString(value.SubStr(pathSize)));
    }

    TString ToString() const {
        return ObjectKeyPrefix ? "s3:" + ::ToString(ConfigPath.size()) + ":" + ConfigPath + *ObjectKeyPrefix : ConfigPath;
    }

    static TString GetDisplayName(const TString& storageId) {
        return storageId.StartsWith("s3:") ? FromString(storageId).GetConfigPath() : storageId;
    }

    friend IOutputStream& operator<<(IOutputStream& out, const TExternalStorageId& storageId) {
        return out << storageId.ToString();
    }

    std::strong_ordering operator<=>(const TExternalStorageId& other) const {
        if (std::tie(ConfigPath, ObjectKeyPrefix) < std::tie(other.ConfigPath, other.ObjectKeyPrefix)) {
            return std::strong_ordering::less;
        }

        if (std::tie(ConfigPath, ObjectKeyPrefix) > std::tie(other.ConfigPath, other.ObjectKeyPrefix)) {
            return std::strong_ordering::greater;
        }

        return std::strong_ordering::equal;
    }
    bool operator==(const TExternalStorageId& other) const {
        return ConfigPath == other.ConfigPath && ObjectKeyPrefix == other.ObjectKeyPrefix;
    }
};

}   // namespace NKikimr::NColumnShard::NTiers

template <>
struct THash<NKikimr::NColumnShard::NTiers::TExternalStorageId> {
    inline ui64 operator()(const NKikimr::NColumnShard::NTiers::TExternalStorageId& x) const noexcept {
        return THash<TString>()(x.ToString());
    }
};
