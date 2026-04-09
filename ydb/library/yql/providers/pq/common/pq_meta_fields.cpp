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
    // User key-value metadata attached to the message (see NYdb::NTopic::TMessageMeta::Fields).
    {"message_meta", TPqMetadataField(NUdf::EDataSlot::Json)},
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
    bool includeUserMessageMeta)
{
    if (!includeUserMessageMeta && key == "message_meta") {
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
    bool includeUserMessageMeta)
{
    bool transparent = false;
    const auto key = SkipPqSystemPrefix(sysColumn, &transparent);
    if (!key) {
        return std::nullopt;
    }

    if (!includeUserMessageMeta && *key == "message_meta") {
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

std::vector<TString> GetAllowedPqMetaSysColumns(bool addTransparentPrefix, bool includeUserMessageMeta) {
    std::vector<TString> res;
    res.reserve(PqMetaFields.size());

    for (const auto& [key, field] : PqMetaFields) {
        if (!includeUserMessageMeta && key == "message_meta") {
            continue;
        }
        res.emplace_back(field.GetSysColumnName(key, addTransparentPrefix));
    }

    return res;
}

} // namespace NYql
