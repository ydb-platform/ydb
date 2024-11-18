#include "pq_meta_fields.h"
#include <yql/essentials/minikql/mkql_string_util.h>

#include <unordered_map>

namespace {
    const std::vector<NYql::TMetaFieldDescriptor> PqMetaFields = {
        NYql::TMetaFieldDescriptor("create_time", "_yql_sys_create_time", NYql::NUdf::EDataSlot::Timestamp),
        NYql::TMetaFieldDescriptor("write_time", "_yql_sys_tsp_write_time", NYql::NUdf::EDataSlot::Timestamp),
        NYql::TMetaFieldDescriptor("partition_id", "_yql_sys_partition_id", NYql::NUdf::EDataSlot::Uint64),
        NYql::TMetaFieldDescriptor("offset", "_yql_sys_offset", NYql::NUdf::EDataSlot::Uint64),
        NYql::TMetaFieldDescriptor("message_group_id", "_yql_sys_message_group_id", NYql::NUdf::EDataSlot::String),
        NYql::TMetaFieldDescriptor("seq_no", "_yql_sys_seq_no", NYql::NUdf::EDataSlot::Uint64),
    };
}

namespace NYql {

const TMetaFieldDescriptor* FindPqMetaFieldDescriptorByKey(const TString& key) {
    const auto iter = std::find_if(
        PqMetaFields.begin(),
        PqMetaFields.end(),
        [&](const NYql::TMetaFieldDescriptor& item){ return item.Key == key; });
    if (iter != PqMetaFields.end()) {
        return iter;
    }

    return nullptr;
}

const TMetaFieldDescriptor* FindPqMetaFieldDescriptorBySysColumn(const TString& sysColumn) {
    const auto iter = std::find_if(
        PqMetaFields.begin(),
        PqMetaFields.end(),
        [&](const NYql::TMetaFieldDescriptor& item){ return item.SysColumn == sysColumn; });
    if (iter != PqMetaFields.end()) {
        return iter;
    }

    return nullptr;
}

std::vector<TString> AllowedPqMetaSysColumns() {
    std::vector<TString> res;
    res.reserve(PqMetaFields.size());

    for (const auto& descriptor : PqMetaFields) {
        res.emplace_back(descriptor.SysColumn);
    }

    return res;
}

}
