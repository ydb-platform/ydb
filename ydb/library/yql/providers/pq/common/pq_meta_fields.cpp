#include "pq_meta_fields.h"
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <unordered_map>

namespace {
    const std::vector<NYql::TMetaFieldDescriptor> PqMetaFields = {
        NYql::TMetaFieldDescriptor("CreateTime", "_yql_sys_create_time", NYql::NUdf::EDataSlot::Timestamp),
        NYql::TMetaFieldDescriptor("WriteTime", "_yql_sys_write_time", NYql::NUdf::EDataSlot::Timestamp),
        NYql::TMetaFieldDescriptor("PartitionId", "_yql_sys_partition_id", NYql::NUdf::EDataSlot::Uint64),
        NYql::TMetaFieldDescriptor("Offset", "_yql_sys_offset", NYql::NUdf::EDataSlot::Uint64),
        NYql::TMetaFieldDescriptor("MessageGroupId", "_yql_sys_message_group_id", NYql::NUdf::EDataSlot::String),
        NYql::TMetaFieldDescriptor("SeqNo", "_yql_sys_seq_no", NYql::NUdf::EDataSlot::Uint64),
    };
}

namespace NYql {

const TMetaFieldDescriptor* FindPqMetaFieldDescriptorByCallable(const TString& callableName) {
    const auto iter = std::find_if(
        PqMetaFields.begin(),
        PqMetaFields.end(),
        [&](const NYql::TMetaFieldDescriptor& item){ return item.CallableName == callableName; });
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

std::vector<TString> AllowedPqMetaCallables() {
    std::vector<TString> res;
    res.reserve(PqMetaFields.size());

    for (const auto& descriptor : PqMetaFields) {
        res.emplace_back(descriptor.CallableName);
    }

    return res;
}

}
