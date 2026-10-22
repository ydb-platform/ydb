#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <functional>

namespace NYql::NSpark {

struct TFunction {
    TString BindingName;
    ui32 MinArgs = 0;
    ui32 MaxArgs = 0;
    bool IsAggregate = false;
    TString GetBindingName(ui32 argumentCount) const;
};

struct TAggregateFunction {
    ui32 Arity = 1;
    TVector<TStringBuf> YqlNames;
    TStringBuf PreprocessorBinding;
    TStringBuf PostprocessorBinding;
};

const TAggregateFunction& GetAggregateFunction(TStringBuf name);
const TFunction* FindFunction(const TString& name);
void EnumerateFunctions(const std::function<void(const TString& name, const TString& bindingName)>& callback);

} // namespace NYql::NSpark
