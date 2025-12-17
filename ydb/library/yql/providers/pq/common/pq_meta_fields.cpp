#include "pq_meta_fields.h"
#include <yql/essentials/minikql/mkql_string_util.h>

#include <unordered_map>

namespace NYql {

namespace {

class TPqMetadataField {
public:
    static constexpr char SYS_PREFIX[] = "_yql_sys_";
    static constexpr char TRANSPARENT_PREFIX[] = "tsp_";

    explicit TPqMetadataField(NUdf::EDataSlot type, bool transparent = false)
        : Type(type)
        , Transparent(transparent)
    {}

    TString GetSysColumn(const TString& key, bool allowTransparentColumns) const {
        auto systemPrefix = TStringBuilder() << SYS_PREFIX;
        if (Transparent && allowTransparentColumns) {
            systemPrefix << TRANSPARENT_PREFIX;
        }

        return systemPrefix << key;
    }

    TMetaFieldDescriptor GetDescriptor(const TString& key, bool allowTransparentColumns) const {
        return {
            .Key = key,
            .SysColumn = GetSysColumn(key, allowTransparentColumns),
            .Type = Type,
        };
    }

public:
    const NUdf::EDataSlot Type;
    const bool Transparent;
};

const std::unordered_map<TString, TPqMetadataField> PqMetaFields = {
    {"create_time", TPqMetadataField(NUdf::EDataSlot::Timestamp)},
    {"write_time", TPqMetadataField(NUdf::EDataSlot::Timestamp, /* transparent */ true)},
    {"partition_id", TPqMetadataField(NUdf::EDataSlot::Uint64)},
    {"offset", TPqMetadataField(NUdf::EDataSlot::Uint64)},
    {"message_group_id", TPqMetadataField(NUdf::EDataSlot::String)},
    {"seq_no", TPqMetadataField(NUdf::EDataSlot::Uint64)},
};

} // anonymous namespace

std::optional<TString> SkipPqSystemPrefix(const TString& sysColumn, bool* isTransparent) {
    TStringBuf keyBuf(sysColumn);
    if (!keyBuf.SkipPrefix(TPqMetadataField::SYS_PREFIX)) {
        return std::nullopt;
    }

    const bool transparent = keyBuf.SkipPrefix(TPqMetadataField::TRANSPARENT_PREFIX);
    if (isTransparent) {
        *isTransparent = transparent;
    }

    return TString(keyBuf);
}

std::optional<TMetaFieldDescriptor> FindPqMetaFieldDescriptorByKey(const TString& key, bool allowTransparentColumns) {
    const auto it = PqMetaFields.find(key);
    if (it == PqMetaFields.end()) {
        return std::nullopt;
    }

    return it->second.GetDescriptor(key, allowTransparentColumns);
}

std::optional<TMetaFieldDescriptor> FindPqMetaFieldDescriptorBySysColumn(const TString& sysColumn) {
    bool transparent = false;
    const auto key = SkipPqSystemPrefix(sysColumn, &transparent);
    if (!key) {
        return std::nullopt;
    }

    const auto it = PqMetaFields.find(*key);
    if (it == PqMetaFields.end()) {
        return std::nullopt;
    }

    const auto& metadata = it->second;
    if (transparent && !metadata.Transparent) {
        return std::nullopt;
    }

    return metadata.GetDescriptor(*key, transparent);
}

std::vector<TString> AllowedPqMetaSysColumns(bool allowTransparentColumns) {
    std::vector<TString> res;
    res.reserve(PqMetaFields.size());

    for (const auto& [key, field] : PqMetaFields) {
        res.emplace_back(field.GetSysColumn(key, allowTransparentColumns));
    }

    return res;
}

} // namespace NYql
