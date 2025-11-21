#include "pq_meta_fields.h"
#include <yql/essentials/minikql/mkql_string_util.h>

#include <unordered_map>

namespace NYql {

namespace {

class TPqMetadataField {
public:
    static constexpr char SYS_PREFIX[] = "_yql_sys_";
    static constexpr char TRANSPERENT_PREFIX[] = "tsp_";

    explicit TPqMetadataField(NUdf::EDataSlot type, bool transperent = false)
        : Type(type)
        , Transperent(transperent)
    {}

    TString GetSysColumn(const TString& key, bool allowTransperentColumns) const {
        auto systemPrefix = TStringBuilder() << SYS_PREFIX;
        if (Transperent && allowTransperentColumns) {
            systemPrefix << TRANSPERENT_PREFIX;
        }

        return systemPrefix << key;
    }

    TMetaFieldDescriptor GetDescriptor(const TString& key, bool allowTransperentColumns) const {
        return {
            .Key = key,
            .SysColumn = GetSysColumn(key, allowTransperentColumns),
            .Type = Type,
        };
    }

public:
    const NUdf::EDataSlot Type;
    const bool Transperent;
};

const std::unordered_map<TString, TPqMetadataField> PqMetaFields = {
    {"create_time", TPqMetadataField(NUdf::EDataSlot::Timestamp)},
    {"write_time", TPqMetadataField(NUdf::EDataSlot::Timestamp, /* transperent */ true)},
    {"partition_id", TPqMetadataField(NUdf::EDataSlot::Uint64)},
    {"offset", TPqMetadataField(NUdf::EDataSlot::Uint64)},
    {"message_group_id", TPqMetadataField(NUdf::EDataSlot::String)},
    {"seq_no", TPqMetadataField(NUdf::EDataSlot::Uint64)},
};

} // anonymous namespace

std::optional<TString> SkipPqSystemPrefix(const TString& sysColumn, bool* isTransperent) {
    TStringBuf keyBuf(sysColumn);
    if (!keyBuf.SkipPrefix(TPqMetadataField::SYS_PREFIX)) {
        return std::nullopt;
    }

    const bool transperent = keyBuf.SkipPrefix(TPqMetadataField::TRANSPERENT_PREFIX);
    if (isTransperent) {
        *isTransperent = transperent;
    }

    return TString(keyBuf);
}

std::optional<TMetaFieldDescriptor> FindPqMetaFieldDescriptorByKey(const TString& key, bool allowTransperentColumns) {
    const auto it = PqMetaFields.find(key);
    if (it == PqMetaFields.end()) {
        return std::nullopt;
    }

    return it->second.GetDescriptor(key, allowTransperentColumns);
}

std::optional<TMetaFieldDescriptor> FindPqMetaFieldDescriptorBySysColumn(const TString& sysColumn) {
    bool transperent = false;
    const auto key = SkipPqSystemPrefix(sysColumn, &transperent);
    if (!key) {
        return std::nullopt;
    }

    const auto it = PqMetaFields.find(*key);
    if (it == PqMetaFields.end()) {
        return std::nullopt;
    }

    const auto& metadata = it->second;
    if (transperent && !metadata.Transperent) {
        return std::nullopt;
    }

    return metadata.GetDescriptor(*key, transperent);
}

std::vector<TString> AllowedPqMetaSysColumns(bool allowTransperentColumns) {
    std::vector<TString> res;
    res.reserve(PqMetaFields.size());

    for (const auto& [key, field] : PqMetaFields) {
        res.emplace_back(field.GetSysColumn(key, allowTransperentColumns));
    }

    return res;
}

} // namespace NYql
