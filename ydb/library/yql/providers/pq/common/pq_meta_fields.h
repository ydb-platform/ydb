#pragma once

#include <optional>

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/string.h>
#include <util/string/builder.h>


namespace NYql {

struct TMetaFieldDescriptor {
    const TString Key;
    const TString SysColumn;
    const NUdf::EDataSlot Type;
};

std::optional<TString> SkipPqSystemPrefix(const TString& sysColumn, bool* isTransperent = nullptr);

std::optional<TMetaFieldDescriptor> FindPqMetaFieldDescriptorByKey(const TString& key, bool allowTransperentColumns);

std::optional<TMetaFieldDescriptor> FindPqMetaFieldDescriptorBySysColumn(const TString& sysColumn);

std::vector<TString> AllowedPqMetaSysColumns(bool allowTransperentColumns);

} // namespace NYql
