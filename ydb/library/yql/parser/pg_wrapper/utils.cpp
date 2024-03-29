#include <util/system/compiler.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
}

#undef Max

#include "utils.h"

namespace NYql {

void PrepareVariadicArraySlow(FunctionCallInfoBaseData& callInfo, const NPg::TProcDesc& desc) {
    const auto& elemDesc = NPg::LookupType(desc.VariadicType);

    Datum varArgs[FUNC_MAX_ARGS];
    bool varArgsNulls[FUNC_MAX_ARGS];
    const ui32 nelems = callInfo.nargs - desc.ArgTypes.size();
    Y_ENSURE(nelems >= 1);
    for (ui32 i = desc.ArgTypes.size(); i < callInfo.nargs; ++i) {
        varArgs[i - desc.ArgTypes.size()] = callInfo.args[i].value;
        varArgsNulls[i - desc.ArgTypes.size()] = callInfo.args[i].isnull;
    }

    callInfo.nargs = desc.ArgTypes.size() + 1;
    int dims[MAXDIM];
    int lbs[MAXDIM];
    dims[0] = nelems;
    lbs[0] = 1;

    auto array = construct_md_array(varArgs, varArgsNulls, 1, dims, lbs,
        desc.VariadicType, elemDesc.TypeLen, elemDesc.PassByValue, elemDesc.TypeAlign);

    auto& argDatum = callInfo.args[callInfo.nargs - 1];
    argDatum.value = PointerGetDatum(array);
    argDatum.isnull = false;
}

void FreeVariadicArray(FunctionCallInfoBaseData& callInfo, ui32 originalArgs) {
    pfree(DatumGetPointer(callInfo.args[callInfo.nargs - 1].value));
    callInfo.nargs = originalArgs;
}

}
