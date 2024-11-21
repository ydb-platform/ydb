#pragma once

#include <optional>

#include <yql/essentials/public/udf/udf_data_type.h>
#include <yql/essentials/public/udf/udf_value.h>

#include <util/generic/string.h>
#include <util/string/builder.h>


namespace NYql {

struct TMetaFieldDescriptor {
public:
    TMetaFieldDescriptor(TString key, TString sysColumn, NUdf::EDataSlot type)
        : Key(key)
        , SysColumn(sysColumn)
        , Type(type)
    { }

public:
    const TString Key;
    const TString SysColumn;
    const NUdf::EDataSlot Type;
};

const TMetaFieldDescriptor* FindPqMetaFieldDescriptorByKey(const TString& key);

const TMetaFieldDescriptor* FindPqMetaFieldDescriptorBySysColumn(const TString& sysColumn);

std::vector<TString> AllowedPqMetaSysColumns();

}
