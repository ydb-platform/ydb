#pragma once

#include <optional>

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/string.h>
#include <util/string/builder.h>


namespace NYql {

enum class EMetaFieldType {
    Uint64,
    Timestamp,
    String,
    DictStringString,
};

struct TMetaFieldDescriptor {
    const TString Key;
    const TString SysColumn;
    const EMetaFieldType Type;
};

std::optional<TString> SkipPqSystemPrefix(const TString& sysColumn, bool* isTransparent = nullptr);

std::optional<TMetaFieldDescriptor> GetPqMetaFieldDescriptorByKey(
    const TString& key,
    bool addTransparentPrefix,
    bool includeUserAttributes);

std::optional<TMetaFieldDescriptor> GetPqMetaFieldDescriptorBySysColumn(
    const TString& sysColumn,
    bool includeUserAttributes);

std::vector<TString> GetAllowedPqMetaSysColumns(bool addTransparentPrefix, bool includeUserAttributes);

} // namespace NYql
