#include "comp_factory_utils.h"

#include <yql/essentials/parser/pg_wrapper/pg_include.h>

#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/parser/pg_wrapper/utils.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

TPgArgsExprBuilder::TPgArgsExprBuilder()
    : PgFuncArgsList(nullptr, &free)
{
}

void TPgArgsExprBuilder::Add(ui32 argOid) {
    PgArgNodes.emplace_back();
    auto& p = PgArgNodes.back();
    Zero(p);
    p.xpr.type = T_Param;
    p.paramkind = PARAM_EXTERN;
    p.paramtype = argOid;
    p.paramcollid = DEFAULT_COLLATION_OID;
    p.paramtypmod = -1;
    p.paramid = PgArgNodes.size();
}

Node* TPgArgsExprBuilder::Build(const NPg::TProcDesc& procDesc) {
    PgFuncArgsList.reset((List*)malloc(offsetof(List, initial_elements) + PgArgNodes.size() * sizeof(ListCell)));
    PgFuncArgsList->type = T_List;
    PgFuncArgsList->elements = PgFuncArgsList->initial_elements;
    PgFuncArgsList->length = PgFuncArgsList->max_length = PgArgNodes.size();
    for (size_t i = 0; i < PgArgNodes.size(); ++i) {
        PgFuncArgsList->elements[i].ptr_value = &PgArgNodes[i];
    }

    Zero(PgFuncNode);
    PgFuncNode.xpr.type = T_FuncExpr;
    PgFuncNode.funcid = procDesc.ProcId;
    PgFuncNode.funcresulttype = procDesc.ResultType;
    PgFuncNode.funcretset = procDesc.ReturnSet;
    PgFuncNode.funcvariadic = procDesc.VariadicArgType && procDesc.VariadicArgType != procDesc.VariadicType;
    PgFuncNode.args = PgFuncArgsList.get();
    return (Node*)&PgFuncNode;
}

TPgResolvedCallBase::TPgResolvedCallBase(std::string_view name,
                                         ui32 id,
                                         TVector<TType*>&& argTypes,
                                         TType* returnType,
                                         bool isList,
                                         const TStructType* structType)
    : TPgResolvedCallBase(name, id,
                          [&argTypes]() {
                              TVector<std::optional<ui32>> result(Reserve(argTypes.size()));
                              for (const auto* argType : argTypes) {
                                  if (argType->IsPg()) {
                                      result.push_back(static_cast<const TPgType*>(argType)->GetTypeId());
                                  } else {
                                      result.push_back(std::nullopt);
                                  }
                              }
                              return result;
                          }(),
                          returnType->IsStruct() ? RECORDOID : AS_TYPE(TPgType, returnType)->GetTypeId(),
                          isList, structType)
{
}

TPgResolvedCallBase::TPgResolvedCallBase(std::string_view name,
                                         ui32 id,
                                         const TVector<std::optional<ui32>>& argTypeIds,
                                         ui32 returnTypeOid,
                                         bool isList,
                                         const TStructType* structType)
    : Name(name)
    , Id(id)
    , ProcDesc(NPg::LookupProc(id))
    , RetTypeDesc(NPg::LookupType(returnTypeOid))
{
    Zero(FInfo);
    Y_ENSURE(Id);
    GetPgFuncAddr(Id, FInfo);
    Y_ENSURE(FInfo.fn_retset == isList);
    Y_ENSURE(FInfo.fn_addr);
    ArgDesc.reserve(argTypeIds.size());
    for (ui32 i = 0; i < argTypeIds.size(); ++i) {
        auto type = [&]() {
            if (argTypeIds[i]) {
                return *argTypeIds[i];
            }
            return ProcDesc.ArgTypes[i];
        }();
        ArgDesc.emplace_back(NPg::LookupType(type));
    }
    Y_ENSURE(ArgDesc.size() == argTypeIds.size());
    Y_ENSURE(ArgDesc.size() <= FUNC_MAX_ARGS);
    for (size_t i = 0; i < ArgDesc.size(); ++i) {
        ArgsExprBuilder.Add(ArgDesc[i].TypeId);
    }

    FInfo.fn_expr = ArgsExprBuilder.Build(ProcDesc);

    if (structType) {
        StructTypeDesc.reserve(structType->GetMembersCount());
        for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
            auto itemType = structType->GetMemberType(i);
            auto type = AS_TYPE(NKikimr::NMiniKQL::TPgType, itemType)->GetTypeId();
            StructTypeDesc.emplace_back(NPg::LookupType(type));
        }
    }
}

TPgResolvedCallBase::TPgResolvedCallBase(std::string_view name,
                                         ui32 id,
                                         const TVector<ui32>& argTypeIds,
                                         ui32 returnTypeOid)
    : TPgResolvedCallBase(name, id,
                          [&argTypeIds]() {
                              TVector<std::optional<ui32>> result;
                              result.reserve(argTypeIds.size());
                              for (ui32 typeId : argTypeIds) {
                                  result.push_back(typeId);
                              }
                              return result;
                          }(),
                          returnTypeOid, /*isList=*/false, /*structType=*/nullptr)
{
}

TPgConst::TPgConst(ui32 typeId, std::string_view value)
    : TypeId(typeId)
    , Value(value)
    , TypeDesc(NPg::LookupType(typeId))
{
    Zero(FInfo);
    ui32 inFuncId = TypeDesc.InFuncId;
    if (TypeDesc.TypeId == TypeDesc.ArrayTypeId) {
        inFuncId = NPg::LookupProc("array_in", {0, 0, 0}).ProcId;
    }

    Y_ENSURE(inFuncId);
    GetPgFuncAddr(inFuncId, FInfo);
    Y_ENSURE(!FInfo.fn_retset);
    Y_ENSURE(FInfo.fn_addr);
    Y_ENSURE(FInfo.fn_nargs >= 1 && FInfo.fn_nargs <= 3);
    TypeIOParam = MakeTypeIOParam(TypeDesc);
}

NUdf::TUnboxedValue TPgConst::ExtractConst(i32 typeMod) const {
    LOCAL_FCINFO(callInfo, 3);
    Zero(*callInfo);
    FmgrInfo copyFmgrInfo = FInfo;
    callInfo->flinfo = &copyFmgrInfo;
    callInfo->nargs = 3;
    callInfo->fncollation = DEFAULT_COLLATION_OID;
    callInfo->isnull = false;
    callInfo->args[0] = {(Datum)Value.c_str(), false};
    callInfo->args[1] = {ObjectIdGetDatum(TypeIOParam), false};
    callInfo->args[2] = {Int32GetDatum(typeMod), false};

    NKikimr::NMiniKQL::TPAllocScope call;
    auto ret = FInfo.fn_addr(callInfo);
    Y_ENSURE(!callInfo->isnull);
    return AnyDatumToPod(ret, TypeDesc.PassByValue);
}

TFunctionCallInfo::TFunctionCallInfo(ui32 numArgs, const FmgrInfo* finfo)
    : NumArgs(numArgs)
    , CopyFmgrInfo(*finfo)
{
    if (!finfo->fn_addr) {
        return;
    }

    MemSize = SizeForFunctionCallInfo(numArgs);
    Ptr = TWithDefaultMiniKQLAlloc::AllocWithSize(MemSize);
    auto& callInfo = Ref();
    Zero(callInfo);
    callInfo.flinfo = &CopyFmgrInfo; // client may mutate fn_extra
    callInfo.nargs = NumArgs;
    callInfo.fncollation = DEFAULT_COLLATION_OID;
}

FunctionCallInfoBaseData& TFunctionCallInfo::Ref() {
    Y_ENSURE(Ptr);
    return *(FunctionCallInfoBaseData*)Ptr;
}

TFunctionCallInfo::~TFunctionCallInfo() {
    if (Ptr) {
        TWithDefaultMiniKQLAlloc::FreeWithSize(Ptr, MemSize);
    }
}

TPgResolvedCallState::TPgResolvedCallState(ui32 numArgs, const FmgrInfo* finfo)
    : CallInfo(numArgs, finfo)
    , Args(numArgs)
{
}

template <bool UseContext>
TPgResolvedCall<UseContext>::TPgResolvedCall(const std::string_view& name,
                                             ui32 id,
                                             TVector<NKikimr::NMiniKQL::TType*>&& argTypes,
                                             NKikimr::NMiniKQL::TType* returnType)
    : TPgResolvedCallBase(name, id, std::move(argTypes), returnType, false, nullptr)
{
}

template <bool UseContext>
TPgResolvedCall<UseContext>::TPgResolvedCall(const std::string_view& name,
                                             ui32 id,
                                             const TVector<std::optional<ui32>>& argTypeIds,
                                             ui32 returnTypeOid)
    : TPgResolvedCallBase(name, id, argTypeIds, returnTypeOid, /*isList=*/false, /*structType=*/nullptr)
{
}

template <bool UseContext>
TPgResolvedCall<UseContext>::TPgResolvedCall(const std::string_view& name,
                                             ui32 id,
                                             const TVector<ui32>& argTypeIds,
                                             ui32 returnTypeOid)
    : TPgResolvedCallBase(name, id, argTypeIds, returnTypeOid)
{
}

template <bool UseContext>
NUdf::TUnboxedValue TPgResolvedCall<UseContext>::DoCall(FunctionCallInfoBaseData& callInfo) const {
    auto ret = this->FInfo.fn_addr(&callInfo);
    if (callInfo.isnull) {
        return NUdf::TUnboxedValue();
    }

    return AnyDatumToPod(ret, this->RetTypeDesc.PassByValue);
}

template <bool UseContext>
NUdf::TUnboxedValue TPgResolvedCall<UseContext>::CallFunction(TPgResolvedCallState& state, NKikimr::NMiniKQL::TUnboxedValueView values) const {
    Y_ENSURE(values.size() == ArgDesc.size());
    return CallFunction(state, [&](size_t index) { return values[index]; });
}

template <bool UseContext>
THolder<TPgResolvedCallState> TPgResolvedCall<UseContext>::CreateState() const {
    return MakeHolder<TPgResolvedCallState>(GetArgCount(), &FInfo);
}

TPgCast::TPgCast(ui32 sourceId, ui32 targetId, bool hasTypeMod)
    : SourceId(sourceId)
    , TargetId(targetId)
    , SourceTypeDesc(SourceId ? NPg::LookupType(SourceId) : NPg::TTypeDesc())
    , TargetTypeDesc(NPg::LookupType(targetId))
    , IsSourceArray(SourceId && SourceTypeDesc.TypeId == SourceTypeDesc.ArrayTypeId)
    , IsTargetArray(TargetTypeDesc.TypeId == TargetTypeDesc.ArrayTypeId)
    , SourceElemDesc(SourceId ? NPg::LookupType(IsSourceArray ? SourceTypeDesc.ElementTypeId : SourceTypeDesc.TypeId) : NPg::TTypeDesc())
    , TargetElemDesc(NPg::LookupType(IsTargetArray ? TargetTypeDesc.ElementTypeId : TargetTypeDesc.TypeId))
{
    TypeIOParam = MakeTypeIOParam(TargetTypeDesc);

    Zero(FInfo1);
    Zero(FInfo2);
    if (hasTypeMod && SourceId == TargetId && NPg::HasCast(TargetElemDesc.TypeId, TargetElemDesc.TypeId)) {
        const auto& cast = NPg::LookupCast(TargetElemDesc.TypeId, TargetElemDesc.TypeId);

        Y_ENSURE(cast.FunctionId);
        GetPgFuncAddr(cast.FunctionId, FInfo1);
        Y_ENSURE(!FInfo1.fn_retset);
        Y_ENSURE(FInfo1.fn_addr);
        Y_ENSURE(FInfo1.fn_nargs >= 2 && FInfo1.fn_nargs <= 3);
        ConvertLength = true;
        ArrayCast = IsSourceArray;
        return;
    }

    if (SourceId == 0 || SourceId == TargetId) {
        return;
    }

    ui32 funcId;
    ui32 funcId2 = 0;
    if (!NPg::HasCast(SourceElemDesc.TypeId, TargetElemDesc.TypeId) || (IsSourceArray != IsTargetArray)) {
        ArrayCast = IsSourceArray && IsTargetArray;
        if (IsSourceArray && !IsTargetArray) {
            Y_ENSURE(TargetTypeDesc.Category == 'S' || TargetId == UNKNOWNOID);
            funcId = NPg::LookupProc("array_out", {0}).ProcId;
        } else if (IsTargetArray && !IsSourceArray) {
            Y_ENSURE(SourceElemDesc.Category == 'S' || SourceId == UNKNOWNOID);
            funcId = NPg::LookupProc("array_in", {0, 0, 0}).ProcId;
        } else if (SourceElemDesc.Category == 'S' || SourceId == UNKNOWNOID) {
            funcId = TargetElemDesc.InFuncId;
        } else {
            Y_ENSURE(TargetTypeDesc.Category == 'S' || TargetId == UNKNOWNOID);
            funcId = SourceElemDesc.OutFuncId;
        }
    } else {
        Y_ENSURE(IsSourceArray == IsTargetArray);
        ArrayCast = IsSourceArray;

        const auto& cast = NPg::LookupCast(SourceElemDesc.TypeId, TargetElemDesc.TypeId);
        switch (cast.Method) {
            case NPg::ECastMethod::Binary:
                return;
            case NPg::ECastMethod::Function: {
                Y_ENSURE(cast.FunctionId);
                funcId = cast.FunctionId;
                break;
            }
            case NPg::ECastMethod::InOut: {
                funcId = SourceElemDesc.OutFuncId;
                funcId2 = TargetElemDesc.InFuncId;
                break;
            }
        }
    }

    Y_ENSURE(funcId);
    GetPgFuncAddr(funcId, FInfo1);
    Y_ENSURE(!FInfo1.fn_retset);
    Y_ENSURE(FInfo1.fn_addr);
    Y_ENSURE(FInfo1.fn_nargs >= 1 && FInfo1.fn_nargs <= 3);
    Func1Lookup = NPg::LookupProc(funcId);
    Y_ENSURE(Func1Lookup.ArgTypes.size() >= 1 && Func1Lookup.ArgTypes.size() <= 3);
    if (NPg::LookupType(Func1Lookup.ArgTypes[0]).TypeLen == -2 && SourceElemDesc.Category == 'S') {
        ConvertArgToCString = true;
    }

    if (funcId2) {
        Y_ENSURE(funcId2);
        GetPgFuncAddr(funcId2, FInfo2);
        Y_ENSURE(!FInfo2.fn_retset);
        Y_ENSURE(FInfo2.fn_addr);
        Y_ENSURE(FInfo2.fn_nargs == 1);
        Func2Lookup = NPg::LookupProc(funcId2);
        Y_ENSURE(Func2Lookup.ArgTypes.size() == 1);
    }

    if (!funcId2) {
        if (NPg::LookupType(Func1Lookup.ResultType).TypeLen == -2 && TargetElemDesc.Category == 'S') {
            ConvertResFromCString = true;
        }
    } else {
        const auto& Func2ArgType = NPg::LookupType(Func2Lookup.ArgTypes[0]);
        if (NPg::LookupType(Func1Lookup.ResultType).TypeLen == -2 && Func2ArgType.Category == 'S') {
            ConvertResFromCString = true;
        }

        if (NPg::LookupType(Func2Lookup.ResultType).TypeLen == -2 && TargetElemDesc.Category == 'S') {
            ConvertResFromCString2 = true;
        }
    }
}

NUdf::TUnboxedValue TPgCast::Calculate(NUdf::TUnboxedValue value, i32 typeMod, TPgCastState& state) const {
    if (!value) {
        return value;
    }

    if (!FInfo1.fn_addr) {
        // binary compatible
        if (!ArrayCast) {
            return value.Release();
        } else {
            // clone array with new target type in the header
            auto datum = PointerDatumFromPod(value);
            ArrayType* arr = DatumGetArrayTypePCopy(datum);
            ARR_ELEMTYPE(arr) = TargetElemDesc.TypeId;
            return PointerDatumToPod(PointerGetDatum(arr));
        }
    }

    TPAllocScope call;
    if (ArrayCast) {
        auto arr = (ArrayType*)DatumGetPointer(PointerDatumFromPod(value));
        auto ndim = ARR_NDIM(arr);
        auto dims = ARR_DIMS(arr);
        auto lb = ARR_LBOUND(arr);
        auto nitems = ArrayGetNItems(ndim, dims);

        Datum* elems = (Datum*)TWithDefaultMiniKQLAlloc::AllocWithSize(nitems * sizeof(Datum));
        Y_DEFER {
            TWithDefaultMiniKQLAlloc::FreeWithSize(elems, nitems * sizeof(Datum));
        };

        bool* nulls = (bool*)TWithDefaultMiniKQLAlloc::AllocWithSize(nitems);
        Y_DEFER {
            TWithDefaultMiniKQLAlloc::FreeWithSize(nulls, nitems);
        };

        array_iter iter;
        array_iter_setup(&iter, (AnyArrayType*)arr);
        for (ui32 i = 0; i < nitems; ++i) {
            bool isNull;
            auto datum = array_iter_next(&iter, &isNull, i, SourceElemDesc.TypeLen,
                                         SourceElemDesc.PassByValue, SourceElemDesc.TypeAlign);
            if (isNull) {
                nulls[i] = true;
                continue;
            } else {
                nulls[i] = false;
                elems[i] = ConvertDatum(datum, state, typeMod);
            }
        }

        auto ret = construct_md_array(elems, nulls, ndim, dims, lb, TargetElemDesc.TypeId,
                                      TargetElemDesc.TypeLen, TargetElemDesc.PassByValue, TargetElemDesc.TypeAlign);

        return PointerDatumToPod(PointerGetDatum(ret));
    } else {
        auto datum = SourceTypeDesc.PassByValue ? ScalarDatumFromPod(value) : PointerDatumFromPod(value);
        auto ret = ConvertDatum(datum, state, typeMod);
        return AnyDatumToPod(ret, TargetTypeDesc.PassByValue);
    }
}

Datum TPgCast::ConvertDatum(Datum datum, TPgCastState& state, i32 typeMod) const {
    auto& callInfo1 = state.CallInfo1.Ref();
    callInfo1.isnull = false;
    NullableDatum argDatum = {datum, false};
    void* freeCString = nullptr;
    Y_DEFER {
        if (freeCString) {
            pfree(freeCString);
        }
    };

    if (ConvertArgToCString) {
        argDatum.value = (Datum)MakeCString(GetVarBuf((const text*)argDatum.value));
        freeCString = (void*)argDatum.value;
    }

    callInfo1.args[0] = argDatum;
    if (ConvertLength) {
        callInfo1.args[1] = {Int32GetDatum(typeMod), false};
        callInfo1.args[2] = {BoolGetDatum(true), false};
    } else {
        if (FInfo1.fn_nargs == 2) {
            callInfo1.args[1] = {Int32GetDatum(typeMod), false};
        } else {
            callInfo1.args[1] = {ObjectIdGetDatum(TypeIOParam), false};
            callInfo1.args[2] = {Int32GetDatum(typeMod), false};
        }
    }

    void* freeMem = nullptr;
    void* freeMem2 = nullptr;
    Y_DEFER {
        if (freeMem) {
            pfree(freeMem);
        }

        if (freeMem2) {
            pfree(freeMem2);
        }
    };

    {
        auto ret = FInfo1.fn_addr(&callInfo1);
        Y_ENSURE(!callInfo1.isnull);

        if (ConvertResFromCString) {
            freeMem = (void*)ret;
            ret = (Datum)MakeVar((const char*)ret);
        }

        if (FInfo2.fn_addr) {
            auto& callInfo2 = state.CallInfo1.Ref();
            callInfo2.isnull = false;
            NullableDatum argDatum2 = {ret, false};
            callInfo2.args[0] = argDatum2;

            auto ret2 = FInfo2.fn_addr(&callInfo2);
            pfree((void*)ret);

            Y_ENSURE(!callInfo2.isnull);
            ret = ret2;
        }

        if (ConvertResFromCString2) {
            freeMem2 = (void*)ret;
            ret = (Datum)MakeVar((const char*)ret);
        }

        return ret;
    }
}

template class TPgResolvedCall<true>;
template class TPgResolvedCall<false>;

TPgResolvedCallWithCastState::TPgResolvedCallWithCastState(
    const TPgResolvedCall<false>& caller,
    const TVector<TPgCast>& casters)
    : CallState(caller.GetArgCount(), &caller.GetFInfo())
{
    CastStates.reserve(casters.size());
    for (const auto& caster : casters) {
        CastStates.push_back(MakeHolder<TPgCastState>(caster.GetFInfo1(), caster.GetFInfo2()));
    }
}

TPgResolvedCallWithCast::TPgResolvedCallWithCast(
    std::string_view name,
    ui32 procId,
    const TVector<ui32>& expectedArgTypeIds,
    const TVector<ui32>& inputArgTypeIds,
    ui32 resultType)
{
    Call = std::make_unique<TPgResolvedCall<false>>(name, procId, expectedArgTypeIds, resultType);

    Casters.reserve(inputArgTypeIds.size());
    for (size_t i = 0; i < inputArgTypeIds.size(); ++i) {
        Casters.emplace_back(inputArgTypeIds[i], expectedArgTypeIds[i], false);
    }
}

TPgResolvedCallWithCast TPgResolvedCallWithCast::ForProc(
    std::string_view name,
    const TVector<ui32>& inputArgTypeIds)
{
    auto procOrType = NPg::LookupProcWithCasts(TString(name), inputArgTypeIds);

    const auto* procPtr = std::get_if<const NPg::TProcDesc*>(&procOrType);
    MKQL_ENSURE(procPtr, "Type cast case not supported, use TPgCast directly");

    const auto& proc = **procPtr;
    MKQL_ENSURE(inputArgTypeIds.size() <= proc.ArgTypes.size(), "Variadic functions not supported");

    TVector<ui32> expectedArgTypeIds;
    expectedArgTypeIds.reserve(inputArgTypeIds.size());
    for (size_t i = 0; i < inputArgTypeIds.size(); ++i) {
        expectedArgTypeIds.push_back(proc.ArgTypes[i]);
    }

    return TPgResolvedCallWithCast(name, proc.ProcId, expectedArgTypeIds, inputArgTypeIds, proc.ResultType);
}

TPgResolvedCallWithCast TPgResolvedCallWithCast::ForOperator(
    std::string_view operName,
    const TVector<ui32>& inputArgTypeIds)
{
    const auto& oper = NPg::LookupOper(TString(operName), inputArgTypeIds);
    TVector<ui32> expectedArgTypeIds = {oper.LeftType, oper.RightType};
    return TPgResolvedCallWithCast(operName, oper.ProcId, expectedArgTypeIds, inputArgTypeIds, oper.ResultType);
}

TPgResolvedCallWithCastState TPgResolvedCallWithCast::CreateState() const {
    return TPgResolvedCallWithCastState(*Call, Casters);
}

NUdf::TUnboxedValue TPgResolvedCallWithCast::CallFunctionWithCast(
    TPgResolvedCallWithCastState& state,
    NKikimr::NMiniKQL::TUnboxedValueView values) const {
    Y_ENSURE(values.size() == Casters.size());

    return Call->CallFunction(state.CallState, [&](size_t i) -> NUdf::TUnboxedValue {
        return Casters[i].Calculate(values[i], -1, *state.CastStates[i]);
    });
}

} // namespace NYql
