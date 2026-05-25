#include "pq_meta_fields.h"
#include <yql/essentials/minikql/mkql_string_util.h>

#include <unordered_map>

namespace NYql {

namespace {

class TPqMetadataField {
public:
    static constexpr char SYS_PREFIX[] = "_yql_sys_";
    static constexpr char TRANSPARENT_PREFIX[] = "tsp_";

    explicit TPqMetadataField(const EMetaFieldType& type, bool transparent = false)
        : Type(type)
        , Transparent(transparent)
    {}

    TString GetSysColumnName(const TString& key, bool addTransparentPrefix) const {
        auto systemPrefix = TStringBuilder() << SYS_PREFIX;
        if (Transparent && addTransparentPrefix) {
            systemPrefix << TRANSPARENT_PREFIX;
        }

        return systemPrefix << key;
    }

    TMetaFieldDescriptor GetDescriptor(const TString& key, bool addTransparentPrefix) const {
        return {
            .Key = key,
            .SysColumn = GetSysColumnName(key, addTransparentPrefix),
            .Type = Type,
        };
    }

public:
    const EMetaFieldType Type;
    const bool Transparent;
};

const std::unordered_map<TString, TPqMetadataField> PqMetaFields = {
    {"create_time", TPqMetadataField(EMetaFieldType::Timestamp)},
    {"write_time", TPqMetadataField(EMetaFieldType::Timestamp, /* transparent */ true)},
    {"partition_id", TPqMetadataField(EMetaFieldType::Uint64)},
    {"offset", TPqMetadataField(EMetaFieldType::Uint64)},
    {"message_group_id", TPqMetadataField(EMetaFieldType::String)},
    {"seq_no", TPqMetadataField(EMetaFieldType::Uint64)},
    {"user_attributes", TPqMetadataField(EMetaFieldType::DictStringString)},
    {"cluster", TPqMetadataField(EMetaFieldType::String)},
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

std::optional<TMetaFieldDescriptor> GetPqMetaFieldDescriptorByKey(
    const TString& key,
    bool addTransparentPrefix,
    bool includeUserAttributes)
{
    if (!includeUserAttributes && key == "user_attributes") {
        return std::nullopt;
    }
    const auto it = PqMetaFields.find(key);
    if (it == PqMetaFields.end()) {
        return std::nullopt;
    }

    return it->second.GetDescriptor(key, addTransparentPrefix);
}

std::optional<TMetaFieldDescriptor> GetPqMetaFieldDescriptorBySysColumn(
    const TString& sysColumn,
    bool includeUserAttributes)
{
    bool transparent = false;
    const auto key = SkipPqSystemPrefix(sysColumn, &transparent);
    if (!key) {
        return std::nullopt;
    }

    if (!includeUserAttributes && *key == "user_attributes") {
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

std::vector<TString> GetAllowedPqMetaSysColumns(bool addTransparentPrefix, bool includeUserAttributes) {
    std::vector<TString> res;
    res.reserve(PqMetaFields.size());

    for (const auto& [key, field] : PqMetaFields) {
        if (!includeUserAttributes && key == "user_attributes") {
            continue;
        }
        res.emplace_back(field.GetSysColumnName(key, addTransparentPrefix));
    }

    return res;
}

} // namespace NYql
