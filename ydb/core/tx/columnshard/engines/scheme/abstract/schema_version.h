#pragma once

#include <ydb/library/accessor/accessor.h>

#include <util/digest/numeric.h>

#include <tuple>

namespace NKikimr::NOlap {

class TSchemaVersionId {
private:
    YDB_READONLY_DEF(ui64, PresetId);
    YDB_READONLY_DEF(ui64, Version);

public:
    bool operator==(const TSchemaVersionId& other) const {
        return std::tie(PresetId, Version) == std::tie(other.PresetId, other.Version);
    }

    TSchemaVersionId(const ui64 presetId, const ui64 version)
        : PresetId(presetId)
        , Version(version) {
    }
};

}

template <>
struct THash<NKikimr::NOlap::TSchemaVersionId> {
    inline size_t operator()(const NKikimr::NOlap::TSchemaVersionId& key) const {
        return CombineHashes(key.GetPresetId(), key.GetVersion());
    }
};
