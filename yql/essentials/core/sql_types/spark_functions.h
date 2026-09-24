#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <functional>

namespace NYql::NSpark {

struct TSparkFunction {
    TString BindingName;
    ui32 MinArgs = 0;
    ui32 MaxArgs = 0;
    TString GetBindingName(ui32 argumentCount) const;
};

const TSparkFunction* FindFunction(const TString& name);
void EnumerateFunctions(const std::function<void(const TString& name, const TString& bindingName)>& callback);

} // namespace NYql::NSpark
