#pragma once

#include <optional>

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <util/generic/string.h>
#include <util/string/builder.h>


namespace NYql {

struct TMetaFieldDescriptor {
public:
    TMetaFieldDescriptor(TString callableName, TString sysColumn, NUdf::EDataSlot type)
        : CallableName(callableName)
        , SysColumn(sysColumn)
        , Type(type)
    { }

public:
    const TString CallableName;
    const TString SysColumn;
    const NUdf::EDataSlot Type;
};

const TMetaFieldDescriptor* FindPqMetaFieldDescriptorByCallable(const TString& callableName);

const TMetaFieldDescriptor* FindPqMetaFieldDescriptorBySysColumn(const TString& sysColumn);

std::vector<TString> AllowedPqMetaSysColumns();

std::vector<TString> AllowedPqMetaCallables();

}
