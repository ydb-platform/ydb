#include <ydb/library/yql/parser/pg_wrapper/interface/interface.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/computation/presort_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_buffer.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.cpp>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_buf.h>
#include <ydb/library/yql/providers/common/codec/yql_codec_results.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/utils/fp_bits.h>
#include <library/cpp/yson/detail.h>
#include <util/string/split.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#define Sort PG_Sort
#define Unique PG_Unique
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/lsyscache.h"
#include "utils/datetime.h"
#include "nodes/execnodes.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "thread_inits.h"

#undef Abs
#undef Min
#undef Max
#undef TypeName
#undef SortBy
#undef Sort
#undef Unique
#undef LOG
#undef INFO
#undef NOTICE
#undef WARNING
//#undef ERROR
#undef FATAL
#undef PANIC
#undef open
#undef fopen
#undef bind
#undef locale_t
}

#include "arrow.h"

namespace NYql {

using namespace NKikimr::NMiniKQL;

TVPtrHolder TVPtrHolder::Instance;

// use 'false' for native format
static __thread bool NeedCanonizeFp = false;

struct TMainContext {
    MemoryContextData Data;
    MemoryContext PrevCurrentMemoryContext = nullptr;
    MemoryContext PrevErrorContext = nullptr;
    TimestampTz StartTimestamp;
    pg_stack_base_t PrevStackBase;
    TString LastError;
};

NUdf::TUnboxedValue CreatePgString(i32 typeLen, ui32 targetTypeId, TStringBuf data) {
    // typname => 'cstring', typlen => '-2', the only type with typlen == -2
    // typname = > 'text', typlen = > '-1'
    Y_UNUSED(targetTypeId); // todo: verify typeLen
    Y_ENSURE(typeLen == -1 || typeLen == -2);
    switch (typeLen) {
    case -1:
        return PointerDatumToPod((Datum)MakeVar(data));
    case -2:
        return PointerDatumToPod((Datum)MakeCString(data));
    default:
        Y_UNREACHABLE();
    }
}

void *MkqlAllocSetAlloc(MemoryContext context, Size size) {
    auto fullSize = size + PallocHdrSize;
    auto ptr = (char *)MKQLAllocDeprecated(fullSize);
    auto ret = (void*)(ptr + PallocHdrSize);
    *(MemoryContext *)(((char *)ret) - sizeof(void *)) = context;
    ((TAllocState::TListEntry*)ptr)->Link(TlsAllocState->CurrentPAllocList);
    return ret;
}

void MkqlAllocSetFree(MemoryContext context, void* pointer) {
    if (pointer) {
        auto original = (void*)((char*)pointer - PallocHdrSize);
        // remove this block from list
        ((TAllocState::TListEntry*)original)->Unlink();
        MKQLFreeDeprecated(original);
    }
}

void* MkqlAllocSetRealloc(MemoryContext context, void* pointer, Size size) {
    if (!size) {
        MkqlAllocSetFree(context, pointer);
        return nullptr;
    }

    void* ret = MkqlAllocSetAlloc(context, size);
    if (pointer) {
        memmove(ret, pointer, size);
    }

    MkqlAllocSetFree(context, pointer);
    return ret;
}

void MkqlAllocSetReset(MemoryContext context) {
}

void MkqlAllocSetDelete(MemoryContext context) {
}

Size MkqlAllocSetGetChunkSpace(MemoryContext context, void* pointer) {
    return 0;
}

bool MkqlAllocSetIsEmpty(MemoryContext context) {
    return false;
}

void MkqlAllocSetStats(MemoryContext context,
    MemoryStatsPrintFunc printfunc, void *passthru,
    MemoryContextCounters *totals,
    bool print_to_stderr) {
}

void MkqlAllocSetCheck(MemoryContext context) {
}

const MemoryContextMethods MkqlMethods = {
    MkqlAllocSetAlloc,
    MkqlAllocSetFree,
    MkqlAllocSetRealloc,
    MkqlAllocSetReset,
    MkqlAllocSetDelete,
    MkqlAllocSetGetChunkSpace,
    MkqlAllocSetIsEmpty,
    MkqlAllocSetStats
#ifdef MEMORY_CONTEXT_CHECKING
    ,MkqlAllocSetCheck
#endif
};

class TPgConst : public TMutableComputationNode<TPgConst> {
    typedef TMutableComputationNode<TPgConst> TBaseComputation;
public:
    TPgConst(TComputationMutables& mutables, ui32 typeId, const std::string_view& value, IComputationNode* typeMod)
        : TBaseComputation(mutables)
        , TypeId(typeId)
        , Value(value)
        , TypeMod(typeMod)
        , TypeDesc(NPg::LookupType(TypeId))
    {
        Zero(FInfo);
        ui32 inFuncId = TypeDesc.InFuncId;
        if (TypeDesc.TypeId == TypeDesc.ArrayTypeId) {
            inFuncId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
        }

        Y_ENSURE(inFuncId);
        fmgr_info(inFuncId, &FInfo);
        Y_ENSURE(!FInfo.fn_retset);
        Y_ENSURE(FInfo.fn_addr);
        Y_ENSURE(FInfo.fn_nargs >=1 && FInfo.fn_nargs <= 3);
        TypeIOParam = MakeTypeIOParam(TypeDesc);
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        i32 typeMod = -1;
        if (TypeMod) {
            typeMod = DatumGetInt32(ScalarDatumFromPod(TypeMod->GetValue(compCtx)));
        }

        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        FmgrInfo copyFmgrInfo = FInfo;
        callInfo->flinfo = &copyFmgrInfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)Value.c_str(), false };
        callInfo->args[1] = { ObjectIdGetDatum(TypeIOParam), false };
        callInfo->args[2] = { Int32GetDatum(typeMod), false };

        TPAllocScope call;
        auto ret = FInfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return AnyDatumToPod(ret, TypeDesc.PassByValue);
    }

private:
    void RegisterDependencies() const final {
        if (TypeMod) {
            DependsOn(TypeMod);
        }
    }

    const ui32 TypeId;
    const TString Value;
    IComputationNode* const TypeMod;
    const NPg::TTypeDesc TypeDesc;
    FmgrInfo FInfo;
    ui32 TypeIOParam;
};

class TPgInternal0 : public TMutableComputationNode<TPgInternal0> {
    typedef TMutableComputationNode<TPgInternal0> TBaseComputation;
public:
    TPgInternal0(TComputationMutables& mutables)
        : TBaseComputation(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        return ScalarDatumToPod(PointerGetDatum(nullptr));
    }

private:
    void RegisterDependencies() const final {
    }
};

class TFunctionCallInfo {
public:
    TFunctionCallInfo(ui32 numArgs, const FmgrInfo* finfo)
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

    FunctionCallInfoBaseData& Ref() {
        Y_ENSURE(Ptr);
        return *(FunctionCallInfoBaseData*)Ptr;
    }

    ~TFunctionCallInfo() {
        if (Ptr) {
            TWithDefaultMiniKQLAlloc::FreeWithSize(Ptr, MemSize);
        }
    }

    TFunctionCallInfo(const TFunctionCallInfo&) = delete;
    void operator=(const TFunctionCallInfo&) = delete;

private:
    const ui32 NumArgs = 0;
    ui32 MemSize = 0;
    void* Ptr = nullptr;
    FmgrInfo CopyFmgrInfo;
};

class TReturnSetInfo {
public:
    TReturnSetInfo() {
        Ptr = TWithDefaultMiniKQLAlloc::AllocWithSize(sizeof(ReturnSetInfo));
        Zero(Ref());
        Ref().type = T_ReturnSetInfo;
    }

    ~TReturnSetInfo() {
        TWithDefaultMiniKQLAlloc::FreeWithSize(Ptr, sizeof(ReturnSetInfo));
    }

    ReturnSetInfo& Ref() {
        return *static_cast<ReturnSetInfo*>(Ptr);
    }

private:
    void* Ptr = nullptr;
};

class TExprContextHolder {
public:
    TExprContextHolder() {
        Ptr = CreateStandaloneExprContext();
    }

    ExprContext& Ref() {
        return *Ptr;
    }

    ~TExprContextHolder() {
        FreeExprContext(Ptr, true);
    }

private:
    ExprContext* Ptr;
};


template <typename TDerived>
class TPgResolvedCallBase : public TMutableComputationNode<TDerived> {
    typedef TMutableComputationNode<TDerived> TBaseComputation;
public:
    TPgResolvedCallBase(TComputationMutables& mutables, const std::string_view& name, ui32 id,
        TComputationNodePtrVector&& argNodes, TVector<TType*>&& argTypes, bool isList)
        : TBaseComputation(mutables)
        , Name(name)
        , Id(id)
        , ProcDesc(NPg::LookupProc(id))
        , RetTypeDesc(NPg::LookupType(ProcDesc.ResultType))
        , ArgNodes(std::move(argNodes))
        , ArgTypes(std::move(argTypes))
    {
        Zero(FInfo);
        Y_ENSURE(Id);
        fmgr_info(Id, &FInfo);
        Y_ENSURE(FInfo.fn_retset == isList);
        Y_ENSURE(FInfo.fn_addr);
        Y_ENSURE(FInfo.fn_nargs == ArgNodes.size());
        ArgDesc.reserve(ProcDesc.ArgTypes.size());
        for (ui32 i = 0; i < ProcDesc.ArgTypes.size(); ++i) {
            ui32 type;
            // extract real type from input args
            auto argType = ArgTypes[i];
            if (argType->IsPg()) {
                type = static_cast<TPgType*>(argType)->GetTypeId();
            } else {
                // keep original description for nulls
                type = ProcDesc.ArgTypes[i];
            }

            ArgDesc.emplace_back(NPg::LookupType(type));
        }

        Y_ENSURE(ArgDesc.size() == ArgNodes.size());
    }

private:
    void RegisterDependencies() const final {
        for (const auto node : ArgNodes) {
            this->DependsOn(node);
        }
    }

protected:
    const std::string_view Name;
    const ui32 Id;
    FmgrInfo FInfo;
    const NPg::TProcDesc ProcDesc;
    const NPg::TTypeDesc RetTypeDesc;
    const TComputationNodePtrVector ArgNodes;
    const TVector<TType*> ArgTypes;
    TVector<NPg::TTypeDesc> ArgDesc;
};

struct TPgResolvedCallState : public TComputationValue<TPgResolvedCallState> {
    TPgResolvedCallState(TMemoryUsageInfo* memInfo, ui32 numArgs, const FmgrInfo* finfo)
        : TComputationValue(memInfo)
        , CallInfo(numArgs, finfo)
    {
    }

    TFunctionCallInfo CallInfo;
};

template <bool UseContext>
class TPgResolvedCall : public TPgResolvedCallBase<TPgResolvedCall<UseContext>> {
    typedef TPgResolvedCallBase<TPgResolvedCall<UseContext>> TBaseComputation;
public:
    TPgResolvedCall(TComputationMutables& mutables, const std::string_view& name, ui32 id,
        TComputationNodePtrVector&& argNodes, TVector<TType*>&& argTypes)
        : TBaseComputation(mutables, name, id, std::move(argNodes), std::move(argTypes), false)
        , StateIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto& state = this->GetState(compCtx);
        auto& callInfo = state.CallInfo.Ref();
        if constexpr (UseContext) {
            callInfo.context = (Node*)TlsAllocState->CurrentContext;
        }

        callInfo.isnull = false;
        for (ui32 i = 0; i < this->ArgNodes.size(); ++i) {
            auto value = this->ArgNodes[i]->GetValue(compCtx);
            NullableDatum argDatum = { 0, false };
            if (!value) {
                if (this->FInfo.fn_strict) {
                    return NUdf::TUnboxedValuePod();
                }

                argDatum.isnull = true;
            } else {
                argDatum.value = this->ArgDesc[i].PassByValue ?
                    ScalarDatumFromPod(value) :
                    PointerDatumFromPod(value);
            }

            callInfo.args[i] = argDatum;
        }

        if constexpr (!UseContext) {
            TPAllocScope call;
            return this->DoCall(callInfo);
        }

        if constexpr (UseContext) {
            return this->DoCall(callInfo);
        }
    }

private:
    NUdf::TUnboxedValuePod DoCall(FunctionCallInfoBaseData& callInfo) const {
        auto ret = this->FInfo.fn_addr(&callInfo);
        if (callInfo.isnull) {
            return NUdf::TUnboxedValuePod();
        }

        return AnyDatumToPod(ret, this->RetTypeDesc.PassByValue);
    }

    TPgResolvedCallState& GetState(TComputationContext& compCtx) const {
        auto& result = compCtx.MutableValues[this->StateIndex];
        if (!result.HasValue()) {
            result = compCtx.HolderFactory.Create<TPgResolvedCallState>(this->ArgNodes.size(), &this->FInfo);
        }

        return *static_cast<TPgResolvedCallState*>(result.AsBoxed().Get());
    }

    const ui32 StateIndex;
};

class TPgResolvedMultiCall : public TPgResolvedCallBase<TPgResolvedMultiCall> {
    typedef TPgResolvedCallBase<TPgResolvedMultiCall> TBaseComputation;
private:
    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, const std::string_view& name, const TUnboxedValueVector& args,
                const TVector<NPg::TTypeDesc>& argDesc, const NPg::TTypeDesc& retTypeDesc, const FmgrInfo* fInfo)
                : TComputationValue<TIterator>(memInfo)
                , Name(name)
                , Args(args)
                , ArgDesc(argDesc)
                , RetTypeDesc(retTypeDesc)
                , CallInfo(argDesc.size(), fInfo)
            {
                auto& callInfo = CallInfo.Ref();
                callInfo.resultinfo = (fmNodePtr)&RSInfo.Ref();
                ((ReturnSetInfo*)callInfo.resultinfo)->econtext = &ExprContextHolder.Ref();
                for (ui32 i = 0; i < args.size(); ++i) {
                    const auto& value = args[i];
                    NullableDatum argDatum = { 0, false };
                    if (!value) {
                        argDatum.isnull = true;
                    } else {
                        argDatum.value = ArgDesc[i].PassByValue ?
                            ScalarDatumFromPod(value) :
                            PointerDatumFromPod(value);
                    }

                    callInfo.args[i] = argDatum;
                }
            }

            ~TIterator() {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (IsFinished) {
                    return false;
                }

                auto& callInfo = CallInfo.Ref();
                callInfo.isnull = false;
                auto ret = callInfo.flinfo->fn_addr(&callInfo);
                if (RSInfo.Ref().isDone == ExprEndResult) {
                    IsFinished = true;
                    return false;
                }

                if (callInfo.isnull) {
                    value = NUdf::TUnboxedValuePod();
                } else {
                    value = AnyDatumToPod(ret, RetTypeDesc.PassByValue);
                }

                return true;
            }

            const std::string_view Name;
            TUnboxedValueVector Args;
            const TVector<NPg::TTypeDesc>& ArgDesc;
            const NPg::TTypeDesc& RetTypeDesc;
            TExprContextHolder ExprContextHolder;
            TFunctionCallInfo CallInfo;
            TReturnSetInfo RSInfo;
            bool IsFinished = false;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx,
            const std::string_view& name, TUnboxedValueVector&& args, const TVector<NPg::TTypeDesc>& argDesc,
            const NPg::TTypeDesc& retTypeDesc, const FmgrInfo* fInfo)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , Name(name)
            , Args(args)
            , ArgDesc(argDesc)
            , RetTypeDesc(retTypeDesc)
            , FInfo(fInfo)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(Name, Args, ArgDesc, RetTypeDesc, FInfo);
        }

        TComputationContext& CompCtx;
        const std::string_view Name;
        TUnboxedValueVector Args;
        const TVector<NPg::TTypeDesc>& ArgDesc;
        const NPg::TTypeDesc& RetTypeDesc;
        const FmgrInfo* FInfo;
    };

public:
    TPgResolvedMultiCall(TComputationMutables& mutables, const std::string_view& name, ui32 id,
        TComputationNodePtrVector&& argNodes, TVector<TType*>&& argTypes)
        : TBaseComputation(mutables, name, id, std::move(argNodes), std::move(argTypes), true)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        TUnboxedValueVector args;
        args.reserve(ArgNodes.size());
        for (ui32 i = 0; i < ArgNodes.size(); ++i) {
            auto value = ArgNodes[i]->GetValue(compCtx);
            args.push_back(value);
        }

        return compCtx.HolderFactory.Create<TListValue>(compCtx, Name, std::move(args), ArgDesc, RetTypeDesc, &FInfo);
    }
};

class TPgCast : public TMutableComputationNode<TPgCast> {
    typedef TMutableComputationNode<TPgCast> TBaseComputation;
public:
    TPgCast(TComputationMutables& mutables, ui32 sourceId, ui32 targetId, IComputationNode* arg, IComputationNode* typeMod)
        : TBaseComputation(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , SourceId(sourceId)
        , TargetId(targetId)
        , Arg(arg)
        , TypeMod(typeMod)
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
        if (TypeMod && SourceId == TargetId && NPg::HasCast(TargetElemDesc.TypeId, TargetElemDesc.TypeId)) {
            const auto& cast = NPg::LookupCast(TargetElemDesc.TypeId, TargetElemDesc.TypeId);

            Y_ENSURE(cast.FunctionId);
            fmgr_info(cast.FunctionId, &FInfo1);
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
                funcId = NPg::LookupProc("array_out", { 0 }).ProcId;
            } else if (IsTargetArray && !IsSourceArray) {
                Y_ENSURE(SourceElemDesc.Category == 'S' || SourceId == UNKNOWNOID);
                funcId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
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
        fmgr_info(funcId, &FInfo1);
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
            fmgr_info(funcId2, &FInfo2);
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

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Arg->GetValue(compCtx);
        if (!value) {
            return value.Release();
        }

        i32 typeMod = -1;
        if (TypeMod) {
            typeMod = DatumGetInt32(ScalarDatumFromPod(TypeMod->GetValue(compCtx)));
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
        auto& state = GetState(compCtx);
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
            auto datum = SourceTypeDesc.PassByValue ?
                ScalarDatumFromPod(value) :
                PointerDatumFromPod(value);
            auto ret = ConvertDatum(datum, state, typeMod);
            return AnyDatumToPod(ret, TargetTypeDesc.PassByValue);
        }
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Arg);
        if (TypeMod) {
            DependsOn(TypeMod);
        }
    }

    struct TState : public TComputationValue<TState> {
        TState(TMemoryUsageInfo* memInfo, const FmgrInfo* finfo1, const FmgrInfo* finfo2)
            : TComputationValue(memInfo)
            , CallInfo1(3, finfo1)
            , CallInfo2(1, finfo2)
        {
        }

        TFunctionCallInfo CallInfo1, CallInfo2;
    };

    TState& GetState(TComputationContext& compCtx) const {
        auto& result = compCtx.MutableValues[StateIndex];
        if (!result.HasValue()) {
            result = compCtx.HolderFactory.Create<TState>(&FInfo1, &FInfo2);
        }

        return *static_cast<TState*>(result.AsBoxed().Get());
    }

    Datum ConvertDatum(Datum datum, TState& state, i32 typeMod) const {
        auto& callInfo1 = state.CallInfo1.Ref();
        callInfo1.isnull = false;
        NullableDatum argDatum = { datum, false };
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
            callInfo1.args[1] = { Int32GetDatum(typeMod), false };
            callInfo1.args[2] = { BoolGetDatum(true), false };
        } else {
            callInfo1.args[1] = { ObjectIdGetDatum(TypeIOParam), false };
            callInfo1.args[2] = { Int32GetDatum(typeMod), false };
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
                NullableDatum argDatum2 = { ret, false };
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

    const ui32 StateIndex;
    const ui32 SourceId;
    const ui32 TargetId;
    IComputationNode* const Arg;
    IComputationNode* const TypeMod;
    const NPg::TTypeDesc SourceTypeDesc;
    const NPg::TTypeDesc TargetTypeDesc;
    const bool IsSourceArray;
    const bool IsTargetArray;
    const NPg::TTypeDesc SourceElemDesc;
    const NPg::TTypeDesc TargetElemDesc;
    FmgrInfo FInfo1, FInfo2;
    NPg::TProcDesc Func1Lookup, Func2Lookup;
    bool ConvertArgToCString = false;
    bool ConvertResFromCString = false;
    bool ConvertResFromCString2 = false;
    ui32 TypeIOParam = 0;
    bool ArrayCast = false;
    bool ConvertLength = false;
};


template <NUdf::EDataSlot Slot>
NUdf::TUnboxedValuePod ConvertToPgValue(NUdf::TUnboxedValuePod value, TMaybe<NUdf::EDataSlot> actualSlot = {}) {
#ifndef NDEBUG
    // todo: improve checks
    if (actualSlot && Slot != *actualSlot) {
        throw yexception() << "Invalid data slot in ConvertToPgValue, expected " << Slot << ", but actual: " << *actualSlot;
    }
#else
    Y_UNUSED(actualSlot);
#endif

    switch (Slot) {
    case NUdf::EDataSlot::Bool:
        return ScalarDatumToPod(BoolGetDatum(value.Get<bool>()));
    case NUdf::EDataSlot::Int16:
        return ScalarDatumToPod(Int16GetDatum(value.Get<i16>()));
    case NUdf::EDataSlot::Int32:
        return ScalarDatumToPod(Int32GetDatum(value.Get<i32>()));
    case NUdf::EDataSlot::Int64:
        return ScalarDatumToPod(Int64GetDatum(value.Get<i64>()));
    case NUdf::EDataSlot::Float:
        return ScalarDatumToPod(Float4GetDatum(value.Get<float>()));
    case NUdf::EDataSlot::Double:
        return ScalarDatumToPod(Float8GetDatum(value.Get<double>()));
    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Utf8: {
        const auto& ref = value.AsStringRef();
        return PointerDatumToPod((Datum)MakeVar(ref));
    }
    default:
        ythrow yexception() << "Unexpected data slot in ConvertToPgValue: " << Slot;
    }
}

template <NUdf::EDataSlot Slot, bool IsCString>
NUdf::TUnboxedValuePod ConvertFromPgValue(NUdf::TUnboxedValuePod value, TMaybe<NUdf::EDataSlot> actualSlot = {}) {
#ifndef NDEBUG
    // todo: improve checks
    if (actualSlot && Slot != *actualSlot) {
        throw yexception() << "Invalid data slot in ConvertFromPgValue, expected " << Slot << ", but actual: " << *actualSlot;
    }
#else
    Y_UNUSED(actualSlot);
#endif

    switch (Slot) {
    case NUdf::EDataSlot::Bool:
        return NUdf::TUnboxedValuePod((bool)DatumGetBool(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Int16:
        return NUdf::TUnboxedValuePod((i16)DatumGetInt16(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Int32:
        return NUdf::TUnboxedValuePod((i32)DatumGetInt32(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Int64:
        return NUdf::TUnboxedValuePod((i64)DatumGetInt64(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Float:
        return NUdf::TUnboxedValuePod((float)DatumGetFloat4(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::Double:
        return NUdf::TUnboxedValuePod((double)DatumGetFloat8(ScalarDatumFromPod(value)));
    case NUdf::EDataSlot::String:
    case NUdf::EDataSlot::Utf8:
        if (IsCString) {
            auto x = (const char*)value.AsBoxed().Get() + PallocHdrSize;
            return MakeString(TStringBuf(x));
        } else {
            auto x = (const text*)((const char*)value.AsBoxed().Get() + PallocHdrSize);
            return MakeString(GetVarBuf(x));
        }
    default:
        ythrow yexception() << "Unexpected data slot in ConvertFromPgValue: " << Slot;
    }
}

NUdf::TUnboxedValuePod ConvertFromPgValue(NUdf::TUnboxedValuePod source, ui32 sourceTypeId, NKikimr::NMiniKQL::TType* targetType) {
    TMaybe<NUdf::EDataSlot> targetDataTypeSlot;
#ifndef NDEBUG
    bool isOptional = false;
    auto targetDataType = UnpackOptionalData(targetType, isOptional);
    YQL_ENSURE(targetDataType);

    targetDataTypeSlot = targetDataType->GetDataSlot();
    if (!source && !isOptional) {
        throw yexception() << "Null value is not allowed for non-optional data type " << *targetType;
    }
#else
    Y_UNUSED(targetType);
#endif

    if (!source) {
        return source;
    }

    switch (sourceTypeId) {
    case BOOLOID:
        return ConvertFromPgValue<NUdf::EDataSlot::Bool, false>(source, targetDataTypeSlot);
    case INT2OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Int16, false>(source, targetDataTypeSlot);
    case INT4OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Int32, false>(source, targetDataTypeSlot);
    case INT8OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Int64, false>(source, targetDataTypeSlot);
    case FLOAT4OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Float, false>(source, targetDataTypeSlot);
    case FLOAT8OID:
        return ConvertFromPgValue<NUdf::EDataSlot::Double, false>(source, targetDataTypeSlot);
    case TEXTOID:
    case VARCHAROID:
        return ConvertFromPgValue<NUdf::EDataSlot::Utf8, false>(source, targetDataTypeSlot);
    case BYTEAOID:
        return ConvertFromPgValue<NUdf::EDataSlot::String, false>(source, targetDataTypeSlot);
    case CSTRINGOID:
        return ConvertFromPgValue<NUdf::EDataSlot::Utf8, true>(source, targetDataTypeSlot);
    default:
        ythrow yexception() << "Unsupported type: " << NPg::LookupType(sourceTypeId).Name;
    }
}

NUdf::TUnboxedValuePod ConvertToPgValue(NUdf::TUnboxedValuePod source, NKikimr::NMiniKQL::TType* sourceType, ui32 targetTypeId) {
    TMaybe<NUdf::EDataSlot> sourceDataTypeSlot;
#ifndef NDEBUG
    bool isOptional = false;
    auto sourceDataType = UnpackOptionalData(sourceType, isOptional);
    YQL_ENSURE(sourceDataType);
    sourceDataTypeSlot = sourceDataType->GetDataSlot();

    if (!source && !isOptional) {
        throw yexception() << "Null value is not allowed for non-optional data type " << *sourceType;
    }
#else
    Y_UNUSED(sourceType);
#endif

    if (!source) {
        return source;
    }

    switch (targetTypeId) {
    case BOOLOID:
        return ConvertToPgValue<NUdf::EDataSlot::Bool>(source, sourceDataTypeSlot);
    case INT2OID:
        return ConvertToPgValue<NUdf::EDataSlot::Int16>(source, sourceDataTypeSlot);
    case INT4OID:
        return ConvertToPgValue<NUdf::EDataSlot::Int32>(source, sourceDataTypeSlot);
    case INT8OID:
        return ConvertToPgValue<NUdf::EDataSlot::Int64>(source, sourceDataTypeSlot);
    case FLOAT4OID:
        return ConvertToPgValue<NUdf::EDataSlot::Float>(source, sourceDataTypeSlot);
    case FLOAT8OID:
        return ConvertToPgValue<NUdf::EDataSlot::Double>(source, sourceDataTypeSlot);
    case TEXTOID:
        return ConvertToPgValue<NUdf::EDataSlot::Utf8>(source, sourceDataTypeSlot);
    case BYTEAOID:
        return ConvertToPgValue<NUdf::EDataSlot::String>(source, sourceDataTypeSlot);
    default:
        ythrow yexception() << "Unsupported type: " << NPg::LookupType(targetTypeId).Name;
    }
}

template <NUdf::EDataSlot Slot, bool IsCString>
class TFromPg : public TMutableComputationNode<TFromPg<Slot, IsCString>> {
    typedef TMutableComputationNode<TFromPg<Slot, IsCString>> TBaseComputation;
public:
    TFromPg(TComputationMutables& mutables, IComputationNode* arg)
        : TBaseComputation(mutables)
        , Arg(arg)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Arg->GetValue(compCtx);
        if (!value) {
            return value.Release();
        }
        return ConvertFromPgValue<Slot, IsCString>(value);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    IComputationNode* const Arg;
};

template <NUdf::EDataSlot Slot>
class TToPg : public TMutableComputationNode<TToPg<Slot>> {
    typedef TMutableComputationNode<TToPg<Slot>> TBaseComputation;
public:
    TToPg(TComputationMutables& mutables, IComputationNode* arg)
        : TBaseComputation(mutables)
        , Arg(arg)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Arg->GetValue(compCtx);
        if (!value) {
            return value.Release();
        }

        return ConvertToPgValue<Slot>(value);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    IComputationNode* const Arg;
};

class TPgArray : public TMutableComputationNode<TPgArray> {
    typedef TMutableComputationNode<TPgArray> TBaseComputation;
public:
    TPgArray(TComputationMutables& mutables, TComputationNodePtrVector&& argNodes, const TVector<TType*>&& argTypes, ui32 arrayType)
        : TBaseComputation(mutables)
        , ArgNodes(std::move(argNodes))
        , ArgTypes(std::move(argTypes))
        , ArrayTypeDesc(NPg::LookupType(arrayType))
        , ElemTypeDesc(NPg::LookupType(ArrayTypeDesc.ElementTypeId))
    {
        ArgDescs.resize(ArgNodes.size());
        for (ui32 i = 0; i < ArgNodes.size(); ++i) {
            if (!ArgTypes[i]->IsNull()) {
                auto type = static_cast<TPgType*>(ArgTypes[i])->GetTypeId();
                ArgDescs[i] = NPg::LookupType(type);
                if (ArgDescs[i].TypeId == ArgDescs[i].ArrayTypeId) {
                    MultiDims = true;
                }
            }
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        TUnboxedValueVector args;
        ui32 nelems = ArgNodes.size();
        args.reserve(nelems);
        for (ui32 i = 0; i < nelems; ++i) {
            auto value = ArgNodes[i]->GetValue(compCtx);
            args.push_back(value);
        }

        Datum* dvalues = (Datum*)TWithDefaultMiniKQLAlloc::AllocWithSize(nelems * sizeof(Datum));
        Y_DEFER {
            TWithDefaultMiniKQLAlloc::FreeWithSize(dvalues, nelems * sizeof(Datum));
        };

        bool *dnulls = (bool*)TWithDefaultMiniKQLAlloc::AllocWithSize(nelems);
        Y_DEFER {
            TWithDefaultMiniKQLAlloc::FreeWithSize(dnulls, nelems);
        };

        TPAllocScope call;
        for (ui32 i = 0; i < nelems; ++i) {
            const auto& value = args[i];
            if (value) {
                dnulls[i] = false;

                dvalues[i] = ArgDescs[i].PassByValue ?
                    ScalarDatumFromPod(value) :
                    PointerDatumFromPod(value);
            } else {
                dnulls[i] = true;
            }
        }

        {
            int ndims = 0;
            int dims[MAXDIM];
            int lbs[MAXDIM];
            if (!MultiDims) {
                // 1D array
                ndims = 1;
                dims[0] = nelems;
                lbs[0] = 1;

                auto result = construct_md_array(dvalues, dnulls, ndims, dims, lbs,
                    ElemTypeDesc.TypeId,
                    ElemTypeDesc.TypeLen,
                    ElemTypeDesc.PassByValue,
                    ElemTypeDesc.TypeAlign);
                return PointerDatumToPod(PointerGetDatum(result));
            }
            else {
                /* Must be nested array expressions */
                auto element_type = ElemTypeDesc.TypeId;
                int nbytes = 0;
                int nitems = 0;
                int outer_nelems = 0;
                int elem_ndims = 0;
                int *elem_dims = NULL;
                int *elem_lbs = NULL;

                bool firstone = true;
                bool havenulls = false;
                bool haveempty = false;
                char **subdata;
                bits8 **subbitmaps;
                int *subbytes;
                int *subnitems;
                int32 dataoffset;
                char *dat;
                int iitem;

                subdata = (char **)palloc(nelems * sizeof(char *));
                subbitmaps = (bits8 **)palloc(nelems * sizeof(bits8 *));
                subbytes = (int *)palloc(nelems * sizeof(int));
                subnitems = (int *)palloc(nelems * sizeof(int));

                /* loop through and get data area from each element */
                for (int elemoff = 0; elemoff < nelems; elemoff++)
                {
                    Datum arraydatum;
                    bool eisnull;
                    ArrayType *array;
                    int this_ndims;

                    arraydatum = dvalues[elemoff];
                    eisnull = dnulls[elemoff];

                    /* temporarily ignore null subarrays */
                    if (eisnull)
                    {
                        haveempty = true;
                        continue;
                    }

                    array = DatumGetArrayTypeP(arraydatum);

                    /* run-time double-check on element type */
                    if (element_type != ARR_ELEMTYPE(array))
                        ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                            errmsg("cannot merge incompatible arrays"),
                            errdetail("Array with element type %s cannot be "
                                "included in ARRAY construct with element type %s.",
                                format_type_be(ARR_ELEMTYPE(array)),
                                format_type_be(element_type))));

                    this_ndims = ARR_NDIM(array);
                    /* temporarily ignore zero-dimensional subarrays */
                    if (this_ndims <= 0)
                    {
                        haveempty = true;
                        continue;
                    }

                    if (firstone)
                    {
                        /* Get sub-array details from first member */
                        elem_ndims = this_ndims;
                        ndims = elem_ndims + 1;
                        if (ndims <= 0 || ndims > MAXDIM)
                            ereport(ERROR,
                            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                                errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
                                    ndims, MAXDIM)));

                        elem_dims = (int *)palloc(elem_ndims * sizeof(int));
                        memcpy(elem_dims, ARR_DIMS(array), elem_ndims * sizeof(int));
                        elem_lbs = (int *)palloc(elem_ndims * sizeof(int));
                        memcpy(elem_lbs, ARR_LBOUND(array), elem_ndims * sizeof(int));

                        firstone = false;
                    }
                    else
                    {
                        /* Check other sub-arrays are compatible */
                        if (elem_ndims != this_ndims ||
                            memcmp(elem_dims, ARR_DIMS(array),
                                elem_ndims * sizeof(int)) != 0 ||
                            memcmp(elem_lbs, ARR_LBOUND(array),
                                elem_ndims * sizeof(int)) != 0)
                            ereport(ERROR,
                            (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                                errmsg("multidimensional arrays must have array "
                                    "expressions with matching dimensions")));
                    }

                    subdata[outer_nelems] = ARR_DATA_PTR(array);
                    subbitmaps[outer_nelems] = ARR_NULLBITMAP(array);
                    subbytes[outer_nelems] = ARR_SIZE(array) - ARR_DATA_OFFSET(array);
                    nbytes += subbytes[outer_nelems];
                    subnitems[outer_nelems] = ArrayGetNItems(this_ndims,
                        ARR_DIMS(array));
                    nitems += subnitems[outer_nelems];
                    havenulls |= ARR_HASNULL(array);
                    outer_nelems++;
                }

                /*
                 * If all items were null or empty arrays, return an empty array;
                 * otherwise, if some were and some weren't, raise error.  (Note: we
                 * must special-case this somehow to avoid trying to generate a 1-D
                 * array formed from empty arrays.  It's not ideal...)
                 */
                if (haveempty)
                {
                    if (ndims == 0) /* didn't find any nonempty array */
                    {
                        return PointerDatumToPod(PointerGetDatum(construct_empty_array(element_type)));
                    }
                    ereport(ERROR,
                        (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
                            errmsg("multidimensional arrays must have array "
                                "expressions with matching dimensions")));
                }

                /* setup for multi-D array */
                dims[0] = outer_nelems;
                lbs[0] = 1;
                for (int i = 1; i < ndims; i++)
                {
                    dims[i] = elem_dims[i - 1];
                    lbs[i] = elem_lbs[i - 1];
                }

                /* check for subscript overflow */
                (void)ArrayGetNItems(ndims, dims);
                ArrayCheckBounds(ndims, dims, lbs);

                if (havenulls)
                {
                    dataoffset = ARR_OVERHEAD_WITHNULLS(ndims, nitems);
                    nbytes += dataoffset;
                }
                else
                {
                    dataoffset = 0; /* marker for no null bitmap */
                    nbytes += ARR_OVERHEAD_NONULLS(ndims);
                }

                ArrayType* result = (ArrayType *)palloc(nbytes);
                SET_VARSIZE(result, nbytes);
                result->ndim = ndims;
                result->dataoffset = dataoffset;
                result->elemtype = element_type;
                memcpy(ARR_DIMS(result), dims, ndims * sizeof(int));
                memcpy(ARR_LBOUND(result), lbs, ndims * sizeof(int));

                dat = ARR_DATA_PTR(result);
                iitem = 0;
                for (int i = 0; i < outer_nelems; i++)
                {
                    memcpy(dat, subdata[i], subbytes[i]);
                    dat += subbytes[i];
                    if (havenulls)
                        array_bitmap_copy(ARR_NULLBITMAP(result), iitem,
                            subbitmaps[i], 0,
                            subnitems[i]);
                    iitem += subnitems[i];
                }

                return PointerDatumToPod(PointerGetDatum(result));
            }
        }
    }

private:
    void RegisterDependencies() const final {
        for (auto arg : ArgNodes) {
            DependsOn(arg);
        }
    }

    TComputationNodePtrVector ArgNodes;
    TVector<TType*> ArgTypes;
    const NPg::TTypeDesc& ArrayTypeDesc;
    const NPg::TTypeDesc& ElemTypeDesc;
    TVector<NPg::TTypeDesc> ArgDescs;
    bool MultiDims = false;
};

template <bool PassByValue, bool IsCString>
class TPgClone : public TMutableComputationNode<TPgClone<PassByValue, IsCString>> {
    typedef TMutableComputationNode<TPgClone<PassByValue, IsCString>> TBaseComputation;
public:
    TPgClone(TComputationMutables& mutables, IComputationNode* input, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables)
        , Input(input)
        , DependentNodes(std::move(dependentNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Input->GetValue(compCtx);
        if constexpr (PassByValue) {
            return value.Release();
        }

        auto datum = PointerDatumFromPod(value);
        if constexpr (IsCString) {
            return PointerDatumToPod((Datum)MakeCString(TStringBuf((const char*)datum)));
        } else {
            return PointerDatumToPod((Datum)MakeVar(GetVarBuf((const text*)datum)));
        }
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Input);
        for (auto arg : DependentNodes) {
            this->DependsOn(arg);
        }
    }

    IComputationNode* const Input;
    TComputationNodePtrVector DependentNodes;
};

struct TFromPgExec {
    TFromPgExec(ui32 sourceId)
        : SourceId(sourceId)
        , IsCString(NPg::LookupType(sourceId).TypeLen == -2)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum inputDatum = batch.values[0];
        Y_ENSURE(inputDatum.is_array());
        const auto& array= *inputDatum.array();
        size_t length = array.length;
        switch (SourceId) {
        case BOOLOID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<ui8>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetBool(inputPtr[i]) ? 1 : 0;
            }
            break;
        }
        case INT2OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<i16>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetInt16(inputPtr[i]);
            }
            break;
        }
        case INT4OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<i32>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetInt32(inputPtr[i]);
            }
            break;
        }
        case INT8OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<i64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetInt64(inputPtr[i]);
            }
            break;
        }
        case FLOAT4OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<float>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetFloat4(inputPtr[i]);
            }
            break;
        }
        case FLOAT8OID: {
            auto inputPtr = array.GetValues<ui64>(1);
            auto outputPtr = res->array()->GetMutableValues<double>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = DatumGetFloat8(inputPtr[i]);
            }
            break;
        }
        case TEXTOID:
        case VARCHAROID:
        case BYTEAOID:
        case CSTRINGOID: {
            NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
            NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), SourceId == BYTEAOID ? arrow::binary() : arrow::utf8(), *ctx->memory_pool(), length);
            for (size_t i = 0; i < length; ++i) {
                auto item = reader.GetItem(array, i);
                if (!item) {
                    builder.Add(NUdf::TBlockItem());
                    continue;
                }

                ui32 len;
                const char* ptr = item.AsStringRef().Data() + sizeof(void*);
                if (IsCString) {
                    len = strlen(ptr);
                } else {
                    len = GetCleanVarSize((const text*)ptr);
                    Y_ENSURE(len + VARHDRSZ + sizeof(void*) == item.AsStringRef().Size());
                    ptr += VARHDRSZ;
                }

                builder.Add(NUdf::TBlockItem(NUdf::TStringRef(ptr, len)));
            }

            *res = builder.Build(true);
            break;
        }
        default:
            ythrow yexception() << "Unsupported type: " << NPg::LookupType(SourceId).Name;
        }
        return arrow::Status::OK();
    }

    const ui32 SourceId;
    const bool IsCString;
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeFromPgKernel(TType* inputType, TType* resultType, ui32 sourceId) {
    const TVector<TType*> argTypes = { inputType };

    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TFromPgExec>(sourceId);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    switch (sourceId) {
    case BOOLOID:
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case FLOAT4OID:
    case FLOAT8OID:
        break;
    case TEXTOID:
    case VARCHAROID:
    case BYTEAOID:
    case CSTRINGOID:
        kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
        break;
    default:
        ythrow yexception() << "Unsupported type: " << NPg::LookupType(sourceId).Name;
    }

    return kernel;
}

struct TToPgExec {
    TToPgExec(ui32 targetId)
        : TargetId(targetId)
        , IsCString(NPg::LookupType(targetId).TypeLen == -2)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum inputDatum = batch.values[0];
        Y_ENSURE(inputDatum.is_array());
        const auto& array= *inputDatum.array();
        size_t length = array.length;
        switch (TargetId) {
        case BOOLOID: {
            auto inputPtr = array.GetValues<ui8>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = BoolGetDatum(inputPtr[i]);
            }
            break;
        }
        case INT2OID: {
            auto inputPtr = array.GetValues<i16>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Int16GetDatum(inputPtr[i]);
            }
            break;
        }
        case INT4OID: {
            auto inputPtr = array.GetValues<i32>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Int32GetDatum(inputPtr[i]);
            }
            break;
        }
        case INT8OID: {
            auto inputPtr = array.GetValues<i64>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Int64GetDatum(inputPtr[i]);
            }
            break;
        }
        case FLOAT4OID: {
            auto inputPtr = array.GetValues<float>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Float4GetDatum(inputPtr[i]);
            }
            break;
        }
        case FLOAT8OID: {
            auto inputPtr = array.GetValues<double>(1);
            auto outputPtr = res->array()->GetMutableValues<ui64>(1);
            for (size_t i = 0; i < length; ++i) {
                outputPtr[i] = Float8GetDatum(inputPtr[i]);
            }
            break;
        }
        case TEXTOID:
        case VARCHAROID:
        case BYTEAOID:
        case CSTRINGOID: {
            NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
            NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::binary(), *ctx->memory_pool(), length);
            std::vector<char> tmp;
            for (size_t i = 0; i < length; ++i) {
                auto item = reader.GetItem(array, i);
                if (!item) {
                    builder.Add(NUdf::TBlockItem());
                    continue;
                }

                ui32 len;
                if (IsCString) {
                    len = sizeof(void*) + 1 + item.AsStringRef().Size();
                    if (Y_UNLIKELY(len < item.AsStringRef().Size())) {
                        ythrow yexception() << "Too long string";
                    }

                    if (tmp.capacity() < len) {
                        tmp.reserve(Max<ui64>(len, tmp.capacity() * 2));
                    }

                    tmp.resize(len);
                    NUdf::ZeroMemoryContext(tmp.data() + sizeof(void*));
                    memcpy(tmp.data() + sizeof(void*), item.AsStringRef().Data(), len - 1 - sizeof(void*));
                    tmp[len - 1] = 0;
                } else {
                    len = sizeof(void*) + VARHDRSZ + item.AsStringRef().Size();
                    if (Y_UNLIKELY(len < item.AsStringRef().Size())) {
                        ythrow yexception() << "Too long string";
                    }

                    if (tmp.capacity() < len) {
                        tmp.reserve(Max<ui64>(len, tmp.capacity() * 2));
                    }

                    tmp.resize(len);
                    NUdf::ZeroMemoryContext(tmp.data() + sizeof(void*));
                    memcpy(tmp.data() + sizeof(void*) + VARHDRSZ, item.AsStringRef().Data(), len - VARHDRSZ);
                    UpdateCleanVarSize((text*)(tmp.data() + sizeof(void*)), item.AsStringRef().Size());
                }

                builder.Add(NUdf::TBlockItem(NUdf::TStringRef(tmp.data(), len)));
            }

            *res = builder.Build(true);
            break;
        }
        default:
            ythrow yexception() << "Unsupported type: " << NPg::LookupType(TargetId).Name;
        }
        return arrow::Status::OK();
    }

    const ui32 TargetId;
    const bool IsCString;
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeToPgKernel(TType* inputType, TType* resultType, ui32 targetId) {
    const TVector<TType*> argTypes = { inputType };

    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TToPgExec>(targetId);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    switch (targetId) {
    case BOOLOID:
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case FLOAT4OID:
    case FLOAT8OID:
        break;
    case TEXTOID:
    case VARCHAROID:
    case BYTEAOID:
    case CSTRINGOID:
        kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
        break;
    default:
        ythrow yexception() << "Unsupported type: " << NPg::LookupType(targetId).Name;
    }

    return kernel;
}

std::shared_ptr<arrow::compute::ScalarKernel> MakePgKernel(TVector<TType*> argTypes, TType* resultType, TExecFunc execFunc, ui32 procId) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [execFunc](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return execFunc(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel->init = [procId](arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs&) {
        auto state = std::make_unique<TPgKernelState>();
        Zero(state->flinfo);
        fmgr_info(procId, &state->flinfo);
        YQL_ENSURE(state->flinfo.fn_addr);
        state->resultinfo = nullptr;
        state->context = nullptr;
        state->fncollation = DEFAULT_COLLATION_OID;
        const auto& procDesc = NPg::LookupProc(procId);
        const auto& retTypeDesc = NPg::LookupType(procDesc.ResultType);
        state->Name = procDesc.Name;
        state->IsFixedResult = retTypeDesc.PassByValue;
        state->IsCStringResult = NPg::LookupType(procDesc.ResultType).TypeLen == -2;
        for (const auto& argTypeId : procDesc.ArgTypes) {
            const auto& argTypeDesc = NPg::LookupType(argTypeId);
            state->IsFixedArg.push_back(argTypeDesc.PassByValue);
        }

        return arrow::Result(std::move(state));
    };

    return kernel;
}

TComputationNodeFactory GetPgFactory() {
    return [] (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            TStringBuf name = callable.GetType()->GetName();
            if (name == "PgConst") {
                const auto typeIdData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto valueData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                ui32 typeId = typeIdData->AsValue().Get<ui32>();
                auto value = valueData->AsValue().AsStringRef();
                IComputationNode* typeMod = nullptr;
                if (callable.GetInputsCount() >= 3) {
                    typeMod = LocateNode(ctx.NodeLocator, callable, 2);
                }

                return new TPgConst(ctx.Mutables, typeId, value, typeMod);
            }

            if (name == "PgInternal0") {
                return new TPgInternal0(ctx.Mutables);
            }

            if (name == "PgResolvedCall") {
                const auto useContextData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto nameData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                const auto idData = AS_VALUE(TDataLiteral, callable.GetInput(2));
                auto useContext = useContextData->AsValue().Get<bool>();
                auto name = nameData->AsValue().AsStringRef();
                auto id = idData->AsValue().Get<ui32>();
                TComputationNodePtrVector argNodes;
                TVector<TType*> argTypes;
                for (ui32 i = 3; i < callable.GetInputsCount(); ++i) {
                    argNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                    argTypes.emplace_back(callable.GetInput(i).GetStaticType());
                }

                const bool isList = callable.GetType()->GetReturnType()->IsList();
                if (isList) {
                    YQL_ENSURE(!useContext);
                    return new TPgResolvedMultiCall(ctx.Mutables, name, id, std::move(argNodes), std::move(argTypes));
                } else {
                    if (useContext) {
                        return new TPgResolvedCall<true>(ctx.Mutables, name, id, std::move(argNodes), std::move(argTypes));
                    } else {
                        return new TPgResolvedCall<false>(ctx.Mutables, name, id, std::move(argNodes), std::move(argTypes));
                    }
                }
            }

            if (name == "BlockPgResolvedCall") {
                const auto nameData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto idData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                auto name = nameData->AsValue().AsStringRef();
                auto id = idData->AsValue().Get<ui32>();
                TVector<IComputationNode*> argNodes;
                TVector<TType*> argTypes;
                for (ui32 i = 2; i < callable.GetInputsCount(); ++i) {
                    argNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                    argTypes.emplace_back(callable.GetInput(i).GetStaticType());
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto execFunc = FindExec(id);
                YQL_ENSURE(execFunc);
                auto kernel = MakePgKernel(argTypes, returnType, execFunc, id);
                return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), std::move(argNodes), argTypes, *kernel, kernel);
            }

            if (name == "PgCast") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto inputType = callable.GetInput(0).GetStaticType();
                ui32 sourceId = 0;
                if (!inputType->IsNull()) {
                    sourceId = AS_TYPE(TPgType, inputType)->GetTypeId();
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto targetId = AS_TYPE(TPgType, returnType)->GetTypeId();
                IComputationNode* typeMod = nullptr;
                if (callable.GetInputsCount() >= 2) {
                    typeMod = LocateNode(ctx.NodeLocator, callable, 1);
                }

                return new TPgCast(ctx.Mutables, sourceId, targetId, arg, typeMod);
            }

            if (name == "FromPg") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto inputType = callable.GetInput(0).GetStaticType();
                ui32 sourceId = AS_TYPE(TPgType, inputType)->GetTypeId();
                switch (sourceId) {
                case BOOLOID:
                    return new TFromPg<NUdf::EDataSlot::Bool, false>(ctx.Mutables, arg);
                case INT2OID:
                    return new TFromPg<NUdf::EDataSlot::Int16, false>(ctx.Mutables, arg);
                case INT4OID:
                    return new TFromPg<NUdf::EDataSlot::Int32, false>(ctx.Mutables, arg);
                case INT8OID:
                    return new TFromPg<NUdf::EDataSlot::Int64, false>(ctx.Mutables, arg);
                case FLOAT4OID:
                    return new TFromPg<NUdf::EDataSlot::Float, false>(ctx.Mutables, arg);
                case FLOAT8OID:
                    return new TFromPg<NUdf::EDataSlot::Double, false>(ctx.Mutables, arg);
                case TEXTOID:
                case VARCHAROID:
                    return new TFromPg<NUdf::EDataSlot::Utf8, false>(ctx.Mutables, arg);
                case BYTEAOID:
                    return new TFromPg<NUdf::EDataSlot::String, false>(ctx.Mutables, arg);
                case CSTRINGOID:
                    return new TFromPg<NUdf::EDataSlot::Utf8, true>(ctx.Mutables, arg);
                default:
                    ythrow yexception() << "Unsupported type: " << NPg::LookupType(sourceId).Name;
                }
            }

            if (name == "BlockFromPg") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto inputType = callable.GetInput(0).GetStaticType();
                auto returnType = callable.GetType()->GetReturnType();
                ui32 sourceId = AS_TYPE(TPgType, AS_TYPE(TBlockType, inputType)->GetItemType())->GetTypeId();
                auto kernel = MakeFromPgKernel(inputType, returnType, sourceId);
                return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), { arg }, { inputType }, *kernel, kernel);
            }

            if (name == "ToPg") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto returnType = callable.GetType()->GetReturnType();
                auto targetId = AS_TYPE(TPgType, returnType)->GetTypeId();
                switch (targetId) {
                case BOOLOID:
                    return new TToPg<NUdf::EDataSlot::Bool>(ctx.Mutables, arg);
                case INT2OID:
                    return new TToPg<NUdf::EDataSlot::Int16>(ctx.Mutables, arg);
                case INT4OID:
                    return new TToPg<NUdf::EDataSlot::Int32>(ctx.Mutables, arg);
                case INT8OID:
                    return new TToPg<NUdf::EDataSlot::Int64>(ctx.Mutables, arg);
                case FLOAT4OID:
                    return new TToPg<NUdf::EDataSlot::Float>(ctx.Mutables, arg);
                case FLOAT8OID:
                    return new TToPg<NUdf::EDataSlot::Double>(ctx.Mutables, arg);
                case TEXTOID:
                    return new TToPg<NUdf::EDataSlot::Utf8>(ctx.Mutables, arg);
                case BYTEAOID:
                    return new TToPg<NUdf::EDataSlot::String>(ctx.Mutables, arg);
                default:
                    ythrow yexception() << "Unsupported type: " << NPg::LookupType(targetId).Name;
                }
            }

            if (name == "BlockToPg") {
                auto arg = LocateNode(ctx.NodeLocator, callable, 0);
                auto inputType = callable.GetInput(0).GetStaticType();
                auto returnType = callable.GetType()->GetReturnType();
                auto targetId = AS_TYPE(TPgType, AS_TYPE(TBlockType, returnType)->GetItemType())->GetTypeId();
                auto kernel = MakeToPgKernel(inputType, returnType, targetId);
                return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), { arg }, { inputType }, *kernel, kernel);
            }

            if (name == "PgArray") {
                TComputationNodePtrVector argNodes;
                TVector<TType*> argTypes;
                for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
                    argNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                    argTypes.emplace_back(callable.GetInput(i).GetStaticType());
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto arrayTypeId = AS_TYPE(TPgType, returnType)->GetTypeId();
                return new TPgArray(ctx.Mutables, std::move(argNodes), std::move(argTypes), arrayTypeId);
            }

            if (name == "PgClone") {
                auto input = LocateNode(ctx.NodeLocator, callable, 0);
                TComputationNodePtrVector dependentNodes;
                for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
                    dependentNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto typeId = AS_TYPE(TPgType, returnType)->GetTypeId();
                const auto& desc = NPg::LookupType(typeId);
                if (desc.PassByValue) {
                    return new TPgClone<true, false>(ctx.Mutables, input, std::move(dependentNodes));
                } else if (desc.TypeLen == -1) {
                    return new TPgClone<false, false>(ctx.Mutables, input, std::move(dependentNodes));
                } else {
                    return new TPgClone<false, true>(ctx.Mutables, input, std::move(dependentNodes));
                }
            }

            return nullptr;
        };
}

namespace NCommon {

TString PgValueToNativeText(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    YQL_ENSURE(value); // null could not be represented as text

    TPAllocScope call;
    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto outFuncId = typeInfo.OutFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        outFuncId = NPg::LookupProc("array_out", { 0 }).ProcId;
    }

    char* str = nullptr;
    Y_DEFER {
        if (str) {
            pfree(str);
        }
    };

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(outFuncId);
        fmgr_info(outFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs == 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { typeInfo.PassByValue ?
            ScalarDatumFromPod(value) :
            PointerDatumFromPod(value), false };
        str = (char*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);

        return TString(str);
    }
}

template <typename F>
void PgValueToNativeBinaryImpl(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId, bool needCanonizeFp, F f) {
    YQL_ENSURE(value); // null could not be represented as binary

    const bool oldNeedCanonizeFp = NeedCanonizeFp;
    NeedCanonizeFp = needCanonizeFp;
    Y_DEFER {
        NeedCanonizeFp = oldNeedCanonizeFp;
    };

    TPAllocScope call;
    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto sendFuncId = typeInfo.SendFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        sendFuncId = NPg::LookupProc("array_send", { 0 }).ProcId;
    }

    text* x = nullptr;
    Y_DEFER {
        if (x) {
            pfree(x);
        }
    };

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(sendFuncId);
        fmgr_info(sendFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs == 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { typeInfo.PassByValue ?
            ScalarDatumFromPod(value) :
            PointerDatumFromPod(value), false };

        x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);

        auto s = GetVarBuf(x);
        ui32 len = s.Size();
        f(TStringBuf(s.Data(), s.Size()));
    }
}

TString PgValueToNativeBinary(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    TString result;
    PgValueToNativeBinaryImpl(value, pgTypeId, false, [&result](TStringBuf b) {
        result = b;
    });
    return result;
}

TString PgValueToString(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    YQL_ENSURE(value); // null could not be represented as text

    switch (pgTypeId) {
    case BOOLOID:
        return DatumGetBool(ScalarDatumFromPod(value)) ? "true" : "false";
    case INT2OID:
        return ToString(DatumGetInt16(ScalarDatumFromPod(value)));
    case INT4OID:
        return ToString(DatumGetInt32(ScalarDatumFromPod(value)));
    case INT8OID:
        return ToString(DatumGetInt64(ScalarDatumFromPod(value)));
    case FLOAT4OID:
        return ::FloatToString(DatumGetFloat4(ScalarDatumFromPod(value)));
    case FLOAT8OID:
        return ::FloatToString(DatumGetFloat8(ScalarDatumFromPod(value)));
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        return TString(GetVarBuf(x));
    }
    case CSTRINGOID: {
        return TString((const char*)PointerDatumFromPod(value));
    }
    default:
        return PgValueToNativeText(value, pgTypeId);
    }
}

void WriteYsonValueInTableFormatPg(TOutputBuf& buf, TPgType* type, const NUdf::TUnboxedValuePod& value) {
    using namespace NYson::NDetail;
    if (!value) {
        buf.Write(EntitySymbol);
        return;
    }

    switch (type->GetTypeId()) {
    case BOOLOID:
        buf.Write(DatumGetBool(ScalarDatumFromPod(value)) ? TrueMarker : FalseMarker);
        break;
    case INT2OID:
        buf.Write(Int64Marker);
        buf.WriteVarI64(DatumGetInt16(ScalarDatumFromPod(value)));
        break;
    case INT4OID:
        buf.Write(Int64Marker);
        buf.WriteVarI64(DatumGetInt32(ScalarDatumFromPod(value)));
        break;
    case INT8OID:
        buf.Write(Int64Marker);
        buf.WriteVarI64(DatumGetInt64(ScalarDatumFromPod(value)));
        break;
    case FLOAT4OID: {
        buf.Write(DoubleMarker);
        double val = DatumGetFloat4(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&val, sizeof(val));
        break;
    }
    case FLOAT8OID: {
        buf.Write(DoubleMarker);
        double val = DatumGetFloat8(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&val, sizeof(val));
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        auto s = GetVarBuf(x);
        buf.Write(StringMarker);
        buf.WriteVarI32(s.Size());
        buf.WriteMany(s.Data(), s.Size());
        break;
    }
    case CSTRINGOID: {
        auto s = (const char*)PointerDatumFromPod(value);
        auto len = strlen(s);
        buf.Write(StringMarker);
        buf.WriteVarI32(len);
        buf.WriteMany(s, len);
        break;
    }
    default:
        buf.Write(StringMarker);
        PgValueToNativeBinaryImpl(value, type->GetTypeId(), true, [&buf](TStringBuf b) {
            buf.WriteVarI32(b.Size());
            buf.WriteMany(b.Data(), b.Size());
        });
        break;
    }
}

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, TPgType* type,
    const TVector<ui32>* structPositions) {
    if (!value) {
        writer.OnNull();
        return;
    }

    writer.OnStringScalar(PgValueToString(value, type->GetTypeId()));
}

NUdf::TUnboxedValue ReadYsonValueInTableFormatPg(TPgType* type, char cmd, TInputBuf& buf) {
    using namespace NYson::NDetail;
    if (cmd == EntitySymbol) {
        return NUdf::TUnboxedValuePod();
    }

    switch (type->GetTypeId()) {
    case BOOLOID: {
        YQL_ENSURE(cmd == FalseMarker || cmd == TrueMarker, "Expected either true or false, but got: " << TString(cmd).Quote());
        return ScalarDatumToPod(BoolGetDatum(cmd == TrueMarker));
    }
    case INT2OID: {
        CHECK_EXPECTED(cmd, Int64Marker);
        auto x = i16(buf.ReadVarI64());
        return ScalarDatumToPod(Int16GetDatum(x));
    }
    case INT4OID: {
        CHECK_EXPECTED(cmd, Int64Marker);
        auto x = i32(buf.ReadVarI64());
        return ScalarDatumToPod(Int32GetDatum(x));
    }
    case INT8OID: {
        CHECK_EXPECTED(cmd, Int64Marker);
        auto x = buf.ReadVarI64();
        return ScalarDatumToPod(Int64GetDatum(x));
    }
    case FLOAT4OID: {
        CHECK_EXPECTED(cmd, DoubleMarker);
        double x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Float4GetDatum(x));
    }
    case FLOAT8OID: {
        CHECK_EXPECTED(cmd, DoubleMarker);
        double x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Float8GetDatum(x));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        CHECK_EXPECTED(cmd, StringMarker);
        auto s = buf.ReadYtString();
        auto ret = MakeVar(s);
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        CHECK_EXPECTED(cmd, StringMarker);
        auto s = buf.ReadYtString();
        auto ret = MakeCString(s);
        return PointerDatumToPod((Datum)ret);
    }
    default:
        TPAllocScope call;
        auto s = buf.ReadYtString();
        return PgValueFromNativeBinary(s, type->GetTypeId());
    }
}

NUdf::TUnboxedValue PgValueFromNativeBinary(const TStringBuf binary, ui32 pgTypeId) {
    TPAllocScope call;
    StringInfoData stringInfo;
    stringInfo.data = (char*)binary.Data();
    stringInfo.len = binary.Size();
    stringInfo.maxlen = binary.Size();
    stringInfo.cursor = 0;

    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto typeIOParam = MakeTypeIOParam(typeInfo);
    auto receiveFuncId = typeInfo.ReceiveFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        receiveFuncId = NPg::LookupProc("array_recv", { 0,0,0 }).ProcId;
    }

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(receiveFuncId);
        fmgr_info(receiveFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs >= 1 && finfo.fn_nargs <= 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)&stringInfo, false };
        callInfo->args[1] = { ObjectIdGetDatum(typeIOParam), false };
        callInfo->args[2] = { Int32GetDatum(-1), false };

        auto x = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        if (stringInfo.cursor != stringInfo.len) {
            TStringBuilder errMsg;
            errMsg << "Not all data has been consumed by 'recv' function: " << NPg::LookupProc(receiveFuncId).Name << ", data size: " << stringInfo.len << ", consumed size: " << stringInfo.cursor;
            UdfTerminate(errMsg.c_str());
        }
        return AnyDatumToPod(x, typeInfo.PassByValue);
    }
}

NUdf::TUnboxedValue PgValueFromNativeText(const TStringBuf text, ui32 pgTypeId) {
    TString str{ text };

    TPAllocScope call;
    const auto& typeInfo = NPg::LookupType(pgTypeId);
    auto typeIOParam = MakeTypeIOParam(typeInfo);
    auto inFuncId = typeInfo.InFuncId;
    if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
        inFuncId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
    }

    {
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(inFuncId);
        fmgr_info(inFuncId, &finfo);
        Y_ENSURE(!finfo.fn_retset);
        Y_ENSURE(finfo.fn_addr);
        Y_ENSURE(finfo.fn_nargs >= 1 && finfo.fn_nargs <= 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)str.c_str(), false };
        callInfo->args[1] = { ObjectIdGetDatum(typeIOParam), false };
        callInfo->args[2] = { Int32GetDatum(-1), false };

        auto x = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return AnyDatumToPod(x, typeInfo.PassByValue);
    }
}

NUdf::TUnboxedValue PgValueFromString(const TStringBuf s, ui32 pgTypeId) {
    switch (pgTypeId) {
    case BOOLOID: {
        return ScalarDatumToPod(BoolGetDatum(FromString<bool>(s)));
    }
    case INT2OID: {
        return ScalarDatumToPod(Int16GetDatum(FromString<i16>(s)));
    }
    case INT4OID: {
        return ScalarDatumToPod(Int32GetDatum(FromString<i32>(s)));
    }
    case INT8OID: {
        return ScalarDatumToPod(Int64GetDatum(FromString<i64>(s)));
    }
    case FLOAT4OID: {
        return ScalarDatumToPod(Float4GetDatum(FromString<float>(s)));
    }
    case FLOAT8OID: {
        return ScalarDatumToPod(Float8GetDatum(FromString<double>(s)));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        auto ret = MakeVar(s);
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        auto ret = MakeCString(s);
        return PointerDatumToPod((Datum)ret);
    }
    default:
        return PgValueFromNativeText(s, pgTypeId);
    }
}

NUdf::TUnboxedValue ReadYsonValuePg(TPgType* type, char cmd, TInputBuf& buf) {
    using namespace NYson::NDetail;
    if (cmd == EntitySymbol) {
        return NUdf::TUnboxedValuePod();
    }

    CHECK_EXPECTED(cmd, StringMarker);
    auto s = buf.ReadYtString();
    return PgValueFromString(s, type->GetTypeId());
}

NUdf::TUnboxedValue ReadSkiffPg(TPgType* type, NCommon::TInputBuf& buf) {
    auto marker = buf.Read();
    if (!marker) {
        return NUdf::TUnboxedValue();
    }

    switch (type->GetTypeId()) {
    case BOOLOID: {
        auto x = buf.Read();
        return ScalarDatumToPod(BoolGetDatum(x != 0));
    }
    case INT2OID: {
        i64 x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Int16GetDatum((i16)x));
    }
    case INT4OID: {
        i64 x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Int32GetDatum((i32)x));
    }
    case INT8OID: {
        i64 x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Int64GetDatum(x));
    }
    case FLOAT4OID: {
        double x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Float4GetDatum((float)x));
    }
    case FLOAT8OID: {
        double x;
        buf.ReadMany((char*)&x, sizeof(x));
        return ScalarDatumToPod(Float8GetDatum(x));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        text* s = (text*)palloc(size + VARHDRSZ);
        auto mem = s;
        Y_DEFER {
            if (mem) {
                pfree(mem);
            }
        };

        UpdateCleanVarSize(s, size);
        buf.ReadMany(GetMutableVarData(s), size);
        mem = nullptr;

        return PointerDatumToPod((Datum)s);
    }

    case CSTRINGOID: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        char* s = (char*)palloc(size + 1);
        auto mem = s;
        Y_DEFER {
            if (mem) {
                pfree(mem);
            }
        };

        buf.ReadMany(s, size);
        mem = nullptr;
        s[size] = '\0';

        return PointerDatumToPod((Datum)s);
    }
    default:
        TPAllocScope call;
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        char* s = (char*)TWithDefaultMiniKQLAlloc::AllocWithSize(size);
        Y_DEFER {
            TWithDefaultMiniKQLAlloc::FreeWithSize(s, size);
        };

        buf.ReadMany(s, size);
        return PgValueFromNativeBinary(TStringBuf(s, size), type->GetTypeId());
    }
}

void WriteSkiffPg(TPgType* type, const NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    if (!value) {
        buf.Write('\0');
        return;
    }

    buf.Write('\1');
    switch (type->GetTypeId()) {
    case BOOLOID: {
        char x = DatumGetBool(ScalarDatumFromPod(value));
        buf.Write(x);
        break;
    }
    case INT2OID: {
        i64 x = DatumGetInt16(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case INT4OID: {
        i64 x = DatumGetInt32(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case INT8OID: {
        i64 x = DatumGetInt64(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case FLOAT4OID: {
        double x = DatumGetFloat4(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case FLOAT8OID: {
        double x = DatumGetFloat8(ScalarDatumFromPod(value));
        buf.WriteMany((const char*)&x, sizeof(x));
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        auto s = GetVarBuf(x);
        ui32 len = s.Size();
        buf.WriteMany((const char*)&len, sizeof(len));
        buf.WriteMany(s.Data(), len);
        break;
    }
    case CSTRINGOID: {
        const auto x = (const char*)PointerDatumFromPod(value);
        ui32 len = strlen(x);
        buf.WriteMany((const char*)&len, sizeof(len));
        buf.WriteMany(x, len);
        break;
    }
    default:
        PgValueToNativeBinaryImpl(value, type->GetTypeId(), true, [&buf](TStringBuf b) {
            ui32 len = b.Size();
            buf.WriteMany((const char*)&len, sizeof(len));
            buf.WriteMany(b.Data(), len);
        });
    }
}

extern "C" void ReadSkiffPgValue(TPgType* type, NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf) {
    value = ReadSkiffPg(type, buf);
}

extern "C" void WriteSkiffPgValue(TPgType* type, const NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    WriteSkiffPg(type, value, buf);
}

} // namespace NCommon

namespace {

template<typename TScalarGetter, typename TPointerGetter>
arrow::Datum DoMakePgScalar(const NPg::TTypeDesc& desc, arrow::MemoryPool& pool, const TScalarGetter& getScalar, const TPointerGetter& getPtr) {
    if (desc.PassByValue) {
        return arrow::MakeScalar(getScalar());
    } else {
        const char* ptr = getPtr();
        ui32 size;
        if (desc.TypeLen == -1) {
            size = GetCleanVarSize((const text*)ptr) + VARHDRSZ;
        } else {
            size = strlen(ptr) + 1;
        }

        std::shared_ptr<arrow::Buffer> buffer(ARROW_RESULT(arrow::AllocateBuffer(size + sizeof(void*), &pool)));
        NUdf::ZeroMemoryContext(buffer->mutable_data() + sizeof(void*));
        std::memcpy(buffer->mutable_data() + sizeof(void*), ptr, size);
        return arrow::Datum(std::make_shared<arrow::BinaryScalar>(buffer));
    }
}

} // namespace

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool) {
    return DoMakePgScalar(
        NPg::LookupType(type->GetTypeId()), pool,
        [&value]() { return (uint64_t)ScalarDatumFromPod(value); },
        [&value]() { return (const char*)PointerDatumFromPod(value); }
    );
}

arrow::Datum MakePgScalar(NKikimr::NMiniKQL::TPgType* type, const NUdf::TBlockItem& value, arrow::MemoryPool& pool) {
    return DoMakePgScalar(
        NPg::LookupType(type->GetTypeId()), pool,
        [&value]() { return (uint64_t)ScalarDatumFromItem(value); },
        [&value]() { return (const char*)PointerDatumFromItem(value); }
    );
}

TMaybe<ui32> ConvertToPgType(NUdf::EDataSlot slot) {
    switch (slot) {
    case NUdf::EDataSlot::Bool:
        return BOOLOID;
    case NUdf::EDataSlot::Int16:
        return INT2OID;
    case NUdf::EDataSlot::Int32:
        return INT4OID;
    case NUdf::EDataSlot::Int64:
        return INT8OID;
    case NUdf::EDataSlot::Float:
        return FLOAT4OID;
    case NUdf::EDataSlot::Double:
        return FLOAT8OID;
    case NUdf::EDataSlot::String:
        return BYTEAOID;
    case NUdf::EDataSlot::Utf8:
        return TEXTOID;
    default:
        return Nothing();
    }
}

TMaybe<NUdf::EDataSlot> ConvertFromPgType(ui32 typeId) {
    switch (typeId) {
    case BOOLOID:
        return NUdf::EDataSlot::Bool;
    case INT2OID:
        return NUdf::EDataSlot::Int16;
    case INT4OID:
        return NUdf::EDataSlot::Int32;
    case INT8OID:
        return NUdf::EDataSlot::Int64;
    case FLOAT4OID:
        return NUdf::EDataSlot::Float;
    case FLOAT8OID:
        return NUdf::EDataSlot::Double;
    case BYTEAOID:
        return NUdf::EDataSlot::String;
    case TEXTOID:
        return NUdf::EDataSlot::Utf8;
    }

    return Nothing();
}

bool ParsePgIntervalModifier(const TString& str, i32& ret) {
    auto ustr = to_upper(str);
    if (ustr == "YEAR") {
        ret = INTERVAL_MASK(YEAR);
    } else if (ustr == "MONTH") {
        ret = INTERVAL_MASK(MONTH);
    } else if (ustr == "DAY") {
        ret = INTERVAL_MASK(DAY);
    } else if (ustr == "HOUR") {
        ret = INTERVAL_MASK(HOUR);
    } else if (ustr == "MINUTE") {
        ret = INTERVAL_MASK(MINUTE);
    } else if (ustr == "SECOND") {
        ret = INTERVAL_MASK(SECOND);
    } else if (ustr == "YEAR TO MONTH") {
        ret = INTERVAL_MASK(YEAR) | INTERVAL_MASK(MONTH);
    } else if (ustr == "DAY TO HOUR") {
        ret = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR);
    } else if (ustr == "DAY TO MINUTE") {
        ret = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE);
    } else if (ustr == "DAY TO SECOND") {
        ret = INTERVAL_MASK(DAY) | INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND);
    } else if (ustr == "HOUR TO MINUTE") {
        ret = INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE);
    } else if (ustr == "HOUR TO SECOND") {
        ret = INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND);
    } else if (ustr == "MINUTE TO SECOND") {
        ret = INTERVAL_MASK(MINUTE) | INTERVAL_MASK(SECOND);
    } else {
        return false;
    }

    return true;
}

template<typename TBuf>
void DoPGPack(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuf& buf) {
    switch (type->GetTypeId()) {
    case BOOLOID: {
        const auto x = DatumGetBool(ScalarDatumFromPod(value)) != 0;
        NDetails::PutRawData(x, buf);
        break;
    }
    case INT2OID: {
        const auto x = DatumGetInt16(ScalarDatumFromPod(value));
        NDetails::PackInt16(x, buf);
        break;
    }
    case INT4OID: {
        const auto x = DatumGetInt32(ScalarDatumFromPod(value));
        NDetails::PackInt32(x, buf);
        break;
    }
    case INT8OID: {
        const auto x = DatumGetInt64(ScalarDatumFromPod(value));
        NDetails::PackInt64(x, buf);
        break;
    }
    case FLOAT4OID: {
        auto x = DatumGetFloat4(ScalarDatumFromPod(value));
        if (stable) {
            NYql::CanonizeFpBits<float>(&x);
        }

        NDetails::PutRawData(x, buf);
        break;
    }
    case FLOAT8OID: {
        auto x = DatumGetFloat8(ScalarDatumFromPod(value));
        if (stable) {
            NYql::CanonizeFpBits<double>(&x);
        }

        NDetails::PutRawData(x, buf);
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        auto s = GetVarBuf(x);
        NDetails::PackUInt32(s.Size(), buf);
        buf.Append(s.Data(), s.Size());
        break;
    }
    case CSTRINGOID: {
        const auto x = (const char*)PointerDatumFromPod(value);
        const auto len = strlen(x);
        NDetails::PackUInt32(len, buf);
        buf.Append(x, len);
        break;
    }
    default:
        NYql::NCommon::PgValueToNativeBinaryImpl(value, type->GetTypeId(), stable, [&buf](TStringBuf b) {
            NDetails::PackUInt32(b.Size(), buf);
            buf.Append(b.Data(), b.Size());
        });
    }
}

} // NYql


namespace NKikimr {
namespace NMiniKQL {

using namespace NYql;

ui64 PgValueSize(const NUdf::TUnboxedValuePod& value, i32 typeLen) {
    if (typeLen >= 0) {
        return typeLen;
    }
    Y_ENSURE(typeLen == -1 || typeLen == -2);
    auto datum = PointerDatumFromPod(value);
    if (typeLen == -1) {
        const auto x = (const text*)PointerDatumFromPod(value);
        return GetCleanVarSize(x);
    } else {
        const auto x = (const char*)PointerDatumFromPod(value);
        return strlen(x);
    }
}

ui64 PgValueSize(ui32 pgTypeId, const NUdf::TUnboxedValuePod& value) {
    const auto& typeDesc = NYql::NPg::LookupType(pgTypeId);
    return PgValueSize(value, typeDesc.TypeLen);
}

ui64 PgValueSize(const TPgType* type, const NUdf::TUnboxedValuePod& value) {
    return PgValueSize(type->GetTypeId(), value);
}

void PGPackImpl(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuffer& buf) {
    DoPGPack(stable, type, value, buf);
}

void PGPackImpl(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TPagedBuffer& buf) {
    DoPGPack(stable, type, value, buf);
}

NUdf::TUnboxedValue PGUnpackImpl(const TPgType* type, TStringBuf& buf) {
    NDetails::TChunkedInputBuffer chunked(buf);
    return PGUnpackImpl(type, chunked);
}

NUdf::TUnboxedValue PGUnpackImpl(const TPgType* type, NDetails::TChunkedInputBuffer& buf) {
    switch (type->GetTypeId()) {
    case BOOLOID: {
        const auto x = NDetails::GetRawData<bool>(buf);
        return ScalarDatumToPod(BoolGetDatum(x));
    }
    case INT2OID: {
        const auto x = NDetails::UnpackInt16(buf);
        return ScalarDatumToPod(Int16GetDatum(x));
    }
    case INT4OID: {
        const auto x = NDetails::UnpackInt32(buf);
        return ScalarDatumToPod(Int32GetDatum(x));
    }
    case INT8OID: {
        const auto x = NDetails::UnpackInt64(buf);
        return ScalarDatumToPod(Int64GetDatum(x));
    }
    case FLOAT4OID: {
        const auto x = NDetails::GetRawData<float>(buf);
        return ScalarDatumToPod(Float4GetDatum(x));
    }
    case FLOAT8OID: {
        const auto x = NDetails::GetRawData<double>(buf);
        return ScalarDatumToPod(Float8GetDatum(x));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        auto size = NDetails::UnpackUInt32(buf);
        auto deleter = [](text* ptr) { pfree(ptr); };
        std::unique_ptr<text, decltype(deleter)> ret(MakeVarNotFilled(size));
        buf.CopyTo(GetMutableVarData(ret.get()), size);
        return PointerDatumToPod((Datum)ret.release());
    }
    case CSTRINGOID: {
        auto size = NDetails::UnpackUInt32(buf);
        auto deleter = [](char* ptr) { pfree(ptr); };
        std::unique_ptr<char, decltype(deleter)> ret(MakeCStringNotFilled(size));
        buf.CopyTo(ret.get(), size);
        return PointerDatumToPod((Datum)ret.release());
    }
    default:
        TPAllocScope call;
        auto size = NDetails::UnpackUInt32(buf);
        std::unique_ptr<char[]> tmpBuf(new char[size]);
        buf.CopyTo(tmpBuf.get(), size);
        TStringBuf s{tmpBuf.get(), size};
        return NYql::NCommon::PgValueFromNativeBinary(s, type->GetTypeId());
    }
}

void EncodePresortPGValue(TPgType* type, const NUdf::TUnboxedValue& value, TVector<ui8>& output) {
    switch (type->GetTypeId()) {
    case BOOLOID: {
        const auto x = DatumGetBool(ScalarDatumFromPod(value)) != 0;
        NDetail::EncodeBool<false>(output, x);
        break;
    }
    case INT2OID: {
        const auto x = DatumGetInt16(ScalarDatumFromPod(value));
        NDetail::EncodeSigned<i16, false>(output, x);
        break;
    }
    case INT4OID: {
        const auto x = DatumGetInt32(ScalarDatumFromPod(value));
        NDetail::EncodeSigned<i32, false>(output, x);
        break;
    }
    case INT8OID: {
        const auto x = DatumGetInt64(ScalarDatumFromPod(value));
        NDetail::EncodeSigned<i64, false>(output, x);
        break;
    }
    case FLOAT4OID: {
        const auto x = DatumGetFloat4(ScalarDatumFromPod(value));
        NDetail::EncodeFloating<float, false>(output, x);
        break;
    }
    case FLOAT8OID: {
        const auto x = DatumGetFloat8(ScalarDatumFromPod(value));
        NDetail::EncodeFloating<double, false>(output, x);
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        auto s = GetVarBuf(x);
        NDetail::EncodeString<false>(output, s);
        break;
    }
    case CSTRINGOID: {
        const auto x = (const char*)PointerDatumFromPod(value);
        NDetail::EncodeString<false>(output, x);
        break;
    }
    default:
        NYql::NCommon::PgValueToNativeBinaryImpl(value, type->GetTypeId(), true, [&output](TStringBuf b) {
            NDetail::EncodeString<false>(output, b);
        });
    }
}

NUdf::TUnboxedValue DecodePresortPGValue(TPgType* type, TStringBuf& input, TVector<ui8>& buffer) {
    switch (type->GetTypeId()) {
    case BOOLOID: {
        const auto x = NDetail::DecodeBool<false>(input);
        return ScalarDatumToPod(BoolGetDatum(x));
    }
    case INT2OID: {
        const auto x = NDetail::DecodeSigned<i16, false>(input);
        return ScalarDatumToPod(Int16GetDatum(x));
    }
    case INT4OID: {
        const auto x = NDetail::DecodeSigned<i32, false>(input);
        return ScalarDatumToPod(Int32GetDatum(x));
    }
    case INT8OID: {
        const auto x = NDetail::DecodeSigned<i64, false>(input);
        return ScalarDatumToPod(Int64GetDatum(x));
    }
    case FLOAT4OID: {
        const auto x = NDetail::DecodeFloating<float, false>(input);
        return ScalarDatumToPod(Float4GetDatum(x));
    }
    case FLOAT8OID: {
        const auto x = NDetail::DecodeFloating<double, false>(input);
        return ScalarDatumToPod(Float8GetDatum(x));
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        buffer.clear();
        const auto s = NDetail::DecodeString<false>(input, buffer);
        auto ret = MakeVar(s);
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        buffer.clear();
        const auto s = NDetail::DecodeString<false>(input, buffer);
        auto ret = MakeCString(s);
        return PointerDatumToPod((Datum)ret);
    }
    default:
        buffer.clear();
        const auto s = NDetail::DecodeString<false>(input, buffer);
        return NYql::NCommon::PgValueFromNativeBinary(s, type->GetTypeId());
    }
}

void* PgInitializeContext(const std::string_view& contextType) {
    if (contextType == "Agg") {
        auto ctx = (AggState*)TWithDefaultMiniKQLAlloc::AllocWithSize(sizeof(AggState));
        Zero(*ctx);
        *(NodeTag*)ctx = T_AggState;
        ctx->curaggcontext = (ExprContext*)TWithDefaultMiniKQLAlloc::AllocWithSize(sizeof(ExprContext));
        Zero(*ctx->curaggcontext);
        ctx->curaggcontext->ecxt_per_tuple_memory = (MemoryContext)&((TMainContext*)TlsAllocState->MainContext)->Data;
        return ctx;
    } else if (contextType == "WinAgg") {
        auto ctx = (WindowAggState*)TWithDefaultMiniKQLAlloc::AllocWithSize(sizeof(WindowAggState));
        Zero(*ctx);
        *(NodeTag*)ctx = T_WindowAggState;
        ctx->curaggcontext = (MemoryContext)&((TMainContext*)TlsAllocState->MainContext)->Data;
        return ctx;
    } else {
        ythrow yexception() << "Unsupported context type: " << contextType;
    }
}

void PgDestroyContext(const std::string_view& contextType, void* ctx) {
    if (contextType == "Agg") {
        TWithDefaultMiniKQLAlloc::FreeWithSize(((AggState*)ctx)->curaggcontext, sizeof(ExprContext));
        TWithDefaultMiniKQLAlloc::FreeWithSize(ctx, sizeof(AggState));
    } else if (contextType == "WinAgg") {
        TWithDefaultMiniKQLAlloc::FreeWithSize(ctx, sizeof(WindowAggState));
    } else {
        Y_FAIL("Unsupported context type");
    }
}

template <bool PassByValue, bool IsArray>
class TPgHash : public NUdf::IHash {
public:
    TPgHash(const NYql::NPg::TTypeDesc& typeDesc)
        : TypeDesc(typeDesc)
    {
        auto hashProcId = TypeDesc.HashProcId;
        if constexpr (IsArray) {
            const auto& elemDesc = NYql::NPg::LookupType(TypeDesc.ElementTypeId);
            Y_ENSURE(elemDesc.HashProcId);

            hashProcId = NYql::NPg::LookupProc("hash_array", { 0, 0 }).ProcId;
        }

        Y_ENSURE(hashProcId);;
        Zero(FInfoHash);
        fmgr_info(hashProcId, &FInfoHash);
        Y_ENSURE(!FInfoHash.fn_retset);
        Y_ENSURE(FInfoHash.fn_addr);
        Y_ENSURE(FInfoHash.fn_nargs == 1);
    }

    ui64 Hash(NUdf::TUnboxedValuePod lhs) const override {
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&FInfoHash); // don't copy becase of IHash isn't threadsafe
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        if (!lhs) {
            return 0;
        }

        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };

        auto x = FInfoHash.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetUInt32(x);
    }

private:
    const NYql::NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoHash;
};

NUdf::IHash::TPtr MakePgHash(const NMiniKQL::TPgType* type) {
    const auto& typeDesc = NYql::NPg::LookupType(type->GetTypeId());
    if (typeDesc.PassByValue) {
        return new TPgHash<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgHash<false, true>(typeDesc);
    } else {
        return new TPgHash<false, false>(typeDesc);
    }
}

template <bool PassByValue, bool IsArray>
class TPgCompare : public NUdf::ICompare, public NUdf::TBlockItemComparatorBase<TPgCompare<PassByValue, IsArray>, true> {
public:
    TPgCompare(const NYql::NPg::TTypeDesc& typeDesc)
        : TypeDesc(typeDesc)
    {
        Zero(FInfoLess);
        Zero(FInfoCompare);
        Zero(FInfoEquals);

        auto lessProcId = TypeDesc.LessProcId;
        auto compareProcId = TypeDesc.CompareProcId;
        auto equalProcId = TypeDesc.EqualProcId;
        if constexpr (IsArray) {
            const auto& elemDesc = NYql::NPg::LookupType(TypeDesc.ElementTypeId);
            Y_ENSURE(elemDesc.CompareProcId);

            compareProcId = NYql::NPg::LookupProc("btarraycmp", { 0, 0 }).ProcId;
        } else {
            Y_ENSURE(lessProcId);
            Y_ENSURE(equalProcId);

            fmgr_info(lessProcId, &FInfoLess);
            Y_ENSURE(!FInfoLess.fn_retset);
            Y_ENSURE(FInfoLess.fn_addr);
            Y_ENSURE(FInfoLess.fn_nargs == 2);

            fmgr_info(equalProcId, &FInfoEquals);
            Y_ENSURE(!FInfoEquals.fn_retset);
            Y_ENSURE(FInfoEquals.fn_addr);
            Y_ENSURE(FInfoEquals.fn_nargs == 2);
        }

        Y_ENSURE(compareProcId);
        fmgr_info(compareProcId, &FInfoCompare);
        Y_ENSURE(!FInfoCompare.fn_retset);
        Y_ENSURE(FInfoCompare.fn_addr);
        Y_ENSURE(FInfoCompare.fn_nargs == 2);
    }

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        if constexpr (IsArray) {
            return Compare(lhs, rhs) < 0;
        }

        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&FInfoLess); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        if (!lhs) {
            if (!rhs) {
                return false;
            }

            return true;
        }

        if (!rhs) {
            return false;
        }

        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromPod(rhs) :
            PointerDatumFromPod(rhs), false };

        auto x = FInfoLess.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetBool(x);
    }

    int Compare(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&FInfoCompare); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        if (!lhs) {
            if (!rhs) {
                return 0;
            }

            return -1;
        }

        if (!rhs) {
            return 1;
        }

        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromPod(rhs) :
            PointerDatumFromPod(rhs), false };

        auto x = FInfoCompare.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetInt32(x);
    }

    i64 DoCompare(NUdf::TBlockItem lhs, NUdf::TBlockItem rhs) const {
        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&FInfoCompare); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromItem(lhs) :
            PointerDatumFromItem(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromItem(rhs) :
            PointerDatumFromItem(rhs), false };

        auto x = FInfoCompare.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetInt32(x);
    }

    bool DoEquals(NUdf::TBlockItem lhs, NUdf::TBlockItem rhs) const {
        if constexpr (IsArray) {
            return DoCompare(lhs, rhs) == 0;
        }

        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&FInfoEquals); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromItem(lhs) :
            PointerDatumFromItem(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromItem(rhs) :
            PointerDatumFromItem(rhs), false };

        auto x = FInfoEquals.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetBool(x);
    }

    bool DoLess(NUdf::TBlockItem lhs, NUdf::TBlockItem rhs) const {
        if constexpr (IsArray) {
            return DoCompare(lhs, rhs) < 0;
        }

        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&FInfoLess); // don't copy becase of ICompare isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromItem(lhs) :
            PointerDatumFromItem(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromItem(rhs) :
            PointerDatumFromItem(rhs), false };

        auto x = FInfoLess.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetBool(x);
    }

private:
    const NYql::NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoLess, FInfoCompare, FInfoEquals;
};

NUdf::ICompare::TPtr MakePgCompare(const NMiniKQL::TPgType* type) {
    const auto& typeDesc = NYql::NPg::LookupType(type->GetTypeId());
    if (typeDesc.PassByValue) {
        return new TPgCompare<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgCompare<false, true>(typeDesc);
    } else {
        return new TPgCompare<false, false>(typeDesc);
    }
}

NUdf::IBlockItemComparator::TPtr MakePgItemComparator(ui32 typeId) {
    const auto& typeDesc = NYql::NPg::LookupType(typeId);
    if (typeDesc.PassByValue) {
        return new TPgCompare<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgCompare<false, true>(typeDesc);
    } else {
        return new TPgCompare<false, false>(typeDesc);
    }
}

template <bool PassByValue, bool IsArray>
class TPgEquate: public NUdf::IEquate {
public:
    TPgEquate(const NYql::NPg::TTypeDesc& typeDesc)
        : TypeDesc(typeDesc)
    {
        auto equalProcId = TypeDesc.EqualProcId;
        if constexpr (IsArray) {
            const auto& elemDesc = NYql::NPg::LookupType(TypeDesc.ElementTypeId);
            Y_ENSURE(elemDesc.CompareProcId);

            equalProcId = NYql::NPg::LookupProc("btarraycmp", { 0, 0 }).ProcId;
        }

        Y_ENSURE(equalProcId);

        Zero(FInfoEquate);
        fmgr_info(equalProcId, &FInfoEquate);
        Y_ENSURE(!FInfoEquate.fn_retset);
        Y_ENSURE(FInfoEquate.fn_addr);
        Y_ENSURE(FInfoEquate.fn_nargs == 2);
    }

    bool Equals(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = const_cast<FmgrInfo*>(&FInfoEquate); // don't copy becase of IEquate isn't threadsafe
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        if (!lhs) {
            if (!rhs) {
                return true;
            }

            return false;
        }

        if (!rhs) {
            return false;
        }

        callInfo->args[0] = { PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { PassByValue ?
            ScalarDatumFromPod(rhs) :
            PointerDatumFromPod(rhs), false };

        auto x = FInfoEquate.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        if constexpr (IsArray) {
            return DatumGetInt32(x) == 0;
        }

        return DatumGetBool(x);
    }

private:
    const NYql::NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoEquate;
};

NUdf::IEquate::TPtr MakePgEquate(const TPgType* type) {
    const auto& typeDesc = NYql::NPg::LookupType(type->GetTypeId());
    if (typeDesc.PassByValue) {
        return new TPgEquate<true, false>(typeDesc);
    } else if (typeDesc.TypeId == typeDesc.ArrayTypeId) {
        return new TPgEquate<false, true>(typeDesc);
    } else {
        return new TPgEquate<false, false>(typeDesc);
    }
}

void* PgInitializeMainContext() {
    auto ctx = new TMainContext();
    MemoryContextCreate((MemoryContext)&ctx->Data,
        T_AllocSetContext,
        &MkqlMethods,
        nullptr,
        "mkql");
    ctx->StartTimestamp = GetCurrentTimestamp();
    return ctx;
}

void PgDestroyMainContext(void* ctx) {
    delete (TMainContext*)ctx;
}

void PgAcquireThreadContext(void* ctx) {
    if (ctx) {
        pg_thread_init();
        auto main = (TMainContext*)ctx;
        main->PrevCurrentMemoryContext = CurrentMemoryContext;
        main->PrevErrorContext = ErrorContext;
        CurrentMemoryContext = ErrorContext = (MemoryContext)&main->Data;
        SetParallelStartTimestamps(main->StartTimestamp, main->StartTimestamp);
        main->PrevStackBase = set_stack_base();
        yql_error_report_active = true;
    }
}

void PgReleaseThreadContext(void* ctx) {
    if (ctx) {
        auto main = (TMainContext*)ctx;
        CurrentMemoryContext = main->PrevCurrentMemoryContext;
        ErrorContext = main->PrevErrorContext;
        restore_stack_base(main->PrevStackBase);
        yql_error_report_active = false;
    }
}

extern "C" void yql_prepare_error(const char* msg) {
    auto ctx  = (TMainContext*)TlsAllocState->MainContext;
    ctx->LastError = msg;
}

extern "C" void yql_raise_error() {
    auto ctx  = (TMainContext*)TlsAllocState->MainContext;
    UdfTerminate(ctx->LastError.c_str());
}

} // namespace NMiniKQL
} // namespace NKikimr

namespace NYql {

class TPgBuilderImpl : public NUdf::IPgBuilder {
public:
     NUdf::TUnboxedValue ValueFromText(ui32 typeId, const NUdf::TStringRef& value, NUdf::TStringValue& error) const override {
        try {
            return NCommon::PgValueFromNativeText(static_cast<TStringBuf>(value), typeId);
        } catch (const std::exception& e) {
            error = NUdf::TStringValue(TStringBuf(e.what()));
        }
        return NUdf::TUnboxedValue();
    }

    NUdf::TUnboxedValue ValueFromBinary(ui32 typeId, const NUdf::TStringRef& value, NUdf::TStringValue& error) const override {
        try {
            return NCommon::PgValueFromNativeBinary(static_cast<TStringBuf>(value), typeId);
        } catch (const std::exception& e) {
            error = NUdf::TStringValue(TStringBuf(e.what()));
        }
        return NUdf::TUnboxedValue();
    }

    NUdf::TUnboxedValue ConvertFromPg(NUdf::TUnboxedValue source, ui32 sourceTypeId, const NUdf::TType* targetType) const override {
        auto t = static_cast<const NKikimr::NMiniKQL::TType*>(targetType);
        return ConvertFromPgValue(source, sourceTypeId, const_cast<NKikimr::NMiniKQL::TType*>(t));
    }

    NUdf::TUnboxedValue ConvertToPg(NUdf::TUnboxedValue source, const NUdf::TType* sourceType, ui32 targetTypeId) const override {
        auto t = static_cast<const NKikimr::NMiniKQL::TType*>(sourceType);
        return ConvertToPgValue(source, const_cast<NKikimr::NMiniKQL::TType*>(t), targetTypeId);
    }

    NUdf::TUnboxedValue NewString(i32 typeLen, ui32 targetTypeId, NUdf::TStringRef data) const override {
        return CreatePgString(typeLen, targetTypeId, data);
    }

    NUdf::TStringRef AsCStringBuffer(const NUdf::TUnboxedValue& value) const override {
        auto x = (const char*)value.AsBoxed().Get() + PallocHdrSize;
        return { x, strlen(x) + 1};
    }

    NUdf::TStringRef AsTextBuffer(const NUdf::TUnboxedValue& value) const override {
        auto x = (const text*)((const char*)value.AsBoxed().Get() + PallocHdrSize);
        return { (const char*)x, GetFullVarSize(x) };
    }

    NUdf::TUnboxedValue MakeCString(const char* value) const override {
        auto len = 1 + strlen(value);
        char* ret = (char*)palloc(len);
        memcpy(ret, value, len);
        return PointerDatumToPod((Datum)ret);
    }

    NUdf::TUnboxedValue MakeText(const char* value) const override {
        auto len = GetFullVarSize((const text*)value);
        char* ret = (char*)palloc(len);
        memcpy(ret, value, len);
        return PointerDatumToPod((Datum)ret);
    }
};

std::unique_ptr<NUdf::IPgBuilder> CreatePgBuilder() {
    return std::make_unique<TPgBuilderImpl>();
}

} // namespace NYql

extern "C" {

void yql_canonize_float4(float4* x) {
    if (NYql::NeedCanonizeFp) {
        NYql::CanonizeFpBits<float>(x);
    }
}

extern void yql_canonize_float8(float8* x) {
    if (NYql::NeedCanonizeFp) {
        NYql::CanonizeFpBits<double>(x);
    }
}

void get_type_io_data(Oid typid,
    IOFuncSelector which_func,
    int16 *typlen,
    bool *typbyval,
    char *typalign,
    char *typdelim,
    Oid *typioparam,
    Oid *func) {
    const auto& typeDesc = NYql::NPg::LookupType(typid);
    *typlen = typeDesc.TypeLen;
    *typbyval = typeDesc.PassByValue;
    *typalign = typeDesc.TypeAlign;
    *typdelim = typeDesc.TypeDelim;
    *typioparam = NYql::MakeTypeIOParam(typeDesc);
    switch (which_func) {
    case IOFunc_input:
        *func = typeDesc.InFuncId;
        break;
    case IOFunc_output:
        *func = typeDesc.OutFuncId;
        break;
    case IOFunc_receive:
        *func = typeDesc.ReceiveFuncId;
        break;
    case IOFunc_send:
        *func = typeDesc.SendFuncId;
        break;
    }
}

} // extern "C"

namespace NKikimr::NPg {

constexpr char INTERNAL_TYPE_AND_MOD_SEPARATOR = ':';

class TPgTypeDescriptor
    : public NYql::NPg::TTypeDesc
{
public:
    explicit TPgTypeDescriptor(const NYql::NPg::TTypeDesc& desc)
        : NYql::NPg::TTypeDesc(desc)
    {
        if (TypeId == ArrayTypeId) {
            const auto& typeDesc = NYql::NPg::LookupType(ElementTypeId);
            YdbTypeName = TString("_pg") + typeDesc.Name;
            if (typeDesc.CompareProcId) {
                CompareProcId = NYql::NPg::LookupProc("btarraycmp", { 0, 0 }).ProcId;
            }
            if (typeDesc.HashProcId) {
                HashProcId = NYql::NPg::LookupProc("hash_array", { 0 }).ProcId;
            }
            if (typeDesc.ReceiveFuncId) {
                ReceiveFuncId = NYql::NPg::LookupProc("array_recv", { 0, 0, 0 }).ProcId;
            }
            if (typeDesc.SendFuncId) {
                SendFuncId = NYql::NPg::LookupProc("array_send", { 0 }).ProcId;
            }
            if (typeDesc.InFuncId) {
                InFuncId = NYql::NPg::LookupProc("array_in", { 0, 0, 0 }).ProcId;
            }
            if (typeDesc.OutFuncId) {
                OutFuncId = NYql::NPg::LookupProc("array_out", { 0 }).ProcId;
            }
            if (NYql::NPg::HasCast(ElementTypeId, ElementTypeId) && typeDesc.TypeModInFuncId) {
                NeedsCoercion = true;
                TypeModInFuncId = typeDesc.TypeModInFuncId;
            }
        } else {
            YdbTypeName = TString("pg") + desc.Name;
            StoredSize = TypeLen < 0 ? 0 : TypeLen;
            if (TypeId == NAMEOID) {
                StoredSize = 0; // store 'name' as usual string
            }
            if (NYql::NPg::HasCast(TypeId, TypeId) && TypeModInFuncId) {
                NeedsCoercion = true;
            }
        }
    }

    int Compare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR) const {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datumL = 0, datumR = 0;
        Y_DEFER {
            if (!PassByValue) {
                if (datumL)
                    pfree((void*)datumL);
                if (datumR)
                    pfree((void*)datumR);
            }
        };

        datumL = Receive(dataL, sizeL);
        datumR = Receive(dataR, sizeR);
        FmgrInfo finfo;
        InitFunc(CompareProcId, &finfo, 2, 2);
        LOCAL_FCINFO(callInfo, 2);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 2;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datumL, false };
        callInfo->args[1] = { datumR, false };

        auto result = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetInt32(result);
    }

    ui64 Hash(const char* data, size_t size) const {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datum = 0;
        Y_DEFER {
            if (!PassByValue && datum) {
                pfree((void*)datum);
            }
        };
        datum = Receive(data, size);
        FmgrInfo finfo;
        InitFunc(HashProcId, &finfo, 1, 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datum, false };

        auto result = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetUInt32(result);
    }

    TConvertResult NativeBinaryFromNativeText(const TString& str) const {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datum = 0;
        text* serialized = nullptr;
        Y_DEFER {
            if (!PassByValue && datum) {
                pfree((void*)datum);
            }
            if (serialized) {
                pfree(serialized);
            }
        };
        PG_TRY();
        {
        {
            FmgrInfo finfo;
            InitFunc(InFuncId, &finfo, 1, 3);
            LOCAL_FCINFO(callInfo, 3);
            Zero(*callInfo);
            callInfo->flinfo = &finfo;
            callInfo->nargs = 3;
            callInfo->fncollation = DEFAULT_COLLATION_OID;
            callInfo->isnull = false;
            callInfo->args[0] = { (Datum)str.c_str(), false };
            callInfo->args[1] = { ObjectIdGetDatum(NMiniKQL::MakeTypeIOParam(*this)), false };
            callInfo->args[2] = { Int32GetDatum(-1), false };

            datum = finfo.fn_addr(callInfo);
            Y_ENSURE(!callInfo->isnull);
        }
        FmgrInfo finfo;
        InitFunc(SendFuncId, &finfo, 1, 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datum, false };

        serialized = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return {TString(NMiniKQL::GetVarBuf(serialized)), {}};
    }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error while converting text to binary: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return {"", errMsg};
        }
        PG_END_TRY();
    }

    TConvertResult NativeTextFromNativeBinary(const TString& binary) const {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datum = 0;
        char* str = nullptr;
        Y_DEFER {
            if (!PassByValue && datum) {
                pfree((void*)datum);
            }
            if (str) {
                pfree(str);
            }
        };
        PG_TRY();
        {
        datum = Receive(binary.Data(), binary.Size());
        FmgrInfo finfo;
        InitFunc(OutFuncId, &finfo, 1, 1);
        LOCAL_FCINFO(callInfo, 1);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 1;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datum, false };

        str = (char*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return {TString(str), {}};
        }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error while converting binary to text: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return {"", errMsg};
        }
        PG_END_TRY();
    }

    TTypeModResult ReadTypeMod(const TString& str) const {
        TVector<TString> params;
        ::Split(str, ",", params);

        if (params.size() > 2) {
            TStringBuilder errMsg;
            errMsg << "Error in 'typemodin' function: "
                << NYql::NPg::LookupProc(TypeModInFuncId).Name
                << ", reason: too many parameters";
            return {-1, errMsg};
        }

        TVector<Datum> dvalues;
        TVector<bool> dnulls;
        dnulls.resize(params.size(), false);
        dvalues.reserve(params.size());

        TString textNumberParam;
        if (TypeId == INTERVALOID || TypeId == INTERVALARRAYOID) {
            i32 typmod = -1;
            auto ok = NYql::ParsePgIntervalModifier(params[0], typmod);
            if (!ok) {
                TStringBuilder errMsg;
                errMsg << "Error in 'typemodin' function: "
                    << NYql::NPg::LookupProc(TypeModInFuncId).Name
                    << ", reason: invalid parameter '" << params[0]
                    << "' for type pginterval";
                return {-1, errMsg};
            }
            textNumberParam = Sprintf("%d", typmod);
            dvalues.push_back(PointerGetDatum(textNumberParam.data()));
            if (params.size() > 1) {
                dvalues.push_back(PointerGetDatum(params[1].data()));
            }
        } else {
            for (size_t i = 0; i < params.size(); ++i) {
                dvalues.push_back(PointerGetDatum(params[i].data()));
            }
        }

        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        ArrayType* paramsArray = nullptr;
        Y_DEFER {
            if (paramsArray) {
                pfree(paramsArray);
            }
        };
        PG_TRY();
        {
            int ndims = 0;
            int dims[MAXDIM];
            int lbs[MAXDIM];

            ndims = 1;
            dims[0] = params.size();
            lbs[0] = 1;

            const auto& cstringDesc = NYql::NPg::LookupType(CSTRINGOID);
            paramsArray = construct_md_array(dvalues.data(), dnulls.data(), ndims, dims, lbs,
                cstringDesc.TypeId,
                cstringDesc.TypeLen,
                cstringDesc.PassByValue,
                cstringDesc.TypeAlign);

            FmgrInfo finfo;
            InitFunc(TypeModInFuncId, &finfo, 1, 1);
            LOCAL_FCINFO(callInfo, 1);
            Zero(*callInfo);
            callInfo->flinfo = &finfo;
            callInfo->nargs = 1;
            callInfo->fncollation = DEFAULT_COLLATION_OID;
            callInfo->isnull = false;
            callInfo->args[0] = { PointerGetDatum(paramsArray), false };

            auto result = finfo.fn_addr(callInfo);
            Y_ENSURE(!callInfo->isnull);
            return {DatumGetInt32(result), {}};
        }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error in 'typemodin' function: "
                << NYql::NPg::LookupProc(TypeModInFuncId).Name
                << ", reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return {-1, errMsg};
        }
        PG_END_TRY();
    }

    TMaybe<TString> Validate(const TStringBuf binary) {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;
        Datum datum = 0;
        Y_DEFER {
            if (!PassByValue && datum) {
                pfree((void*)datum);
            }
        };
        PG_TRY();
        {
        datum = Receive(binary.Data(), binary.Size());
        return {};
    }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error in 'recv' function: "
                << NYql::NPg::LookupProc(ReceiveFuncId).Name
                << ", reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return errMsg;
        }
        PG_END_TRY();
    }

    TCoerceResult Coerce(const TStringBuf binary, i32 typmod) {
        return Coerce(true, binary, 0, typmod);
    }

    TCoerceResult Coerce(const NUdf::TUnboxedValuePod& value, i32 typmod) {
        Datum datum = PassByValue ?
            NMiniKQL::ScalarDatumFromPod(value) :
            NMiniKQL::PointerDatumFromPod(value);

        return Coerce(false, {}, datum, typmod);
    }

private:
    TCoerceResult Coerce(bool isSourceBinary, const TStringBuf binary, Datum datum, i32 typmod) {
        NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NMiniKQL::TPAllocScope scope;

        Datum datumCasted = 0;
        TVector<Datum> elems;
        TVector<bool> nulls;
        TVector<Datum> castedElements;
        bool passByValueElem = false;
        text* serialized = nullptr;
        Y_DEFER {
            if (!PassByValue) {
                if (datum && isSourceBinary) {
                    pfree((void*)datum);
                }
                if (datumCasted) {
                    pfree((void*)datumCasted);
                }
            }
            if (IsArray() && !passByValueElem) {
                for (ui32 i = 0; i < castedElements.size(); ++i) {
                    pfree((void*)castedElements[i]);
                }
            }
            if (serialized) {
                pfree(serialized);
            }
        };
        PG_TRY();
        {
            if (isSourceBinary) {
                datum = Receive(binary.Data(), binary.Size());
            }

            if (IsArray()) {
                const auto& typeDesc = NYql::NPg::LookupType(ElementTypeId);
                passByValueElem = typeDesc.PassByValue;

                auto arr = (ArrayType*)DatumGetPointer(datum);
                auto ndim = ARR_NDIM(arr);
                auto dims = ARR_DIMS(arr);
                auto lb = ARR_LBOUND(arr);
                auto nitems = ArrayGetNItems(ndim, dims);

                elems.resize(nitems);
                nulls.resize(nitems);
                castedElements.reserve(nitems);

                array_iter iter;
                array_iter_setup(&iter, (AnyArrayType*)arr);
                for (ui32 i = 0; i < nitems; ++i) {
                    bool isNull;
                    auto datum = array_iter_next(&iter, &isNull, i,
                        typeDesc.TypeLen, typeDesc.PassByValue, typeDesc.TypeAlign);
                    if (isNull) {
                        elems[i] = 0;
                        nulls[i] = true;
                        continue;
                    }
                    elems[i] = CoerceOne(ElementTypeId, datum, typmod);
                    nulls[i] = false;
                    if (elems[i] != datum) {
                        castedElements.push_back(elems[i]);
                    }
                }

                if (!castedElements.empty()) {
                    auto newArray = construct_md_array(elems.data(), nulls.data(), ndim, dims, lb,
                        typeDesc.TypeId, typeDesc.TypeLen, typeDesc.PassByValue, typeDesc.TypeAlign);
                    datumCasted = PointerGetDatum(newArray);
                }
            } else {
                datumCasted = CoerceOne(TypeId, datum, typmod);
                if (datumCasted == datum) {
                    datumCasted = 0;
                }
            }

            if (!datumCasted && isSourceBinary) {
                return {{}, {}};
            } else {
                FmgrInfo finfo;
                InitFunc(SendFuncId, &finfo, 1, 1);
                LOCAL_FCINFO(callInfo, 1);
                Zero(*callInfo);
                callInfo->flinfo = &finfo;
                callInfo->nargs = 1;
                callInfo->fncollation = DEFAULT_COLLATION_OID;
                callInfo->isnull = false;
                callInfo->args[0] = { datumCasted ? datumCasted : datum, false };

                serialized = (text*)finfo.fn_addr(callInfo);
                Y_ENSURE(!callInfo->isnull);
                return {TString(NMiniKQL::GetVarBuf(serialized)), {}};
            }
        }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error while coercing value, reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            return {{}, errMsg};
        }
        PG_END_TRY();
    }

    Datum CoerceOne(ui32 typeId, Datum datum, i32 typmod) const {
        const auto& cast = NYql::NPg::LookupCast(typeId, typeId);

        FmgrInfo finfo;
        InitFunc(cast.FunctionId, &finfo, 2, 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { datum, false };
        callInfo->args[1] = { Int32GetDatum(typmod), false };
        callInfo->args[2] = { BoolGetDatum(false), false };

        auto result = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return result;
    }

    Datum Receive(const char* data, size_t size) const {
        StringInfoData stringInfo;
        stringInfo.data = (char*)data;
        stringInfo.len = size;
        stringInfo.maxlen = size;
        stringInfo.cursor = 0;

        FmgrInfo finfo;
        InitFunc(ReceiveFuncId, &finfo, 1, 3);
        LOCAL_FCINFO(callInfo, 3);
        Zero(*callInfo);
        callInfo->flinfo = &finfo;
        callInfo->nargs = 3;
        callInfo->fncollation = DEFAULT_COLLATION_OID;
        callInfo->isnull = false;
        callInfo->args[0] = { (Datum)&stringInfo, false };
        callInfo->args[1] = { ObjectIdGetDatum(NMiniKQL::MakeTypeIOParam(*this)), false };
        callInfo->args[2] = { Int32GetDatum(-1), false };

        auto result = finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return result;
    }

    bool IsArray() {
        return TypeId == ArrayTypeId;
    }

    static inline void InitFunc(ui32 funcId, FmgrInfo* info, ui32 argCountMin, ui32 argCountMax) {
        Zero(*info);
        Y_ENSURE(funcId);
        fmgr_info(funcId, info);
        Y_ENSURE(info->fn_addr);
        Y_ENSURE(info->fn_nargs >= argCountMin && info->fn_nargs <= argCountMax);
    }

public:
    TString YdbTypeName;
    ui32 StoredSize = 0; // size in local db, 0 for variable size
    bool NeedsCoercion = false;
};

class TPgTypeDescriptors {
public:
    static const TPgTypeDescriptors& Instance() {
        return *Singleton<TPgTypeDescriptors>();
    }

    TPgTypeDescriptors() {
        auto initType = [this] (ui32 pgTypeId, const NYql::NPg::TTypeDesc& type) {
            this->InitType(pgTypeId, type);
        };
        NYql::NPg::EnumTypes(initType);
    }

    const TPgTypeDescriptor* Find(ui32 pgTypeId) const {
        return PgTypeDescriptors.FindPtr(pgTypeId);
    }

    const TPgTypeDescriptor* Find(const TStringBuf name) const {
        auto* id = ByName.FindPtr(name);
        if (id) {
            return Find(*id);
        }
        return {};
    }

private:
    void InitType(ui32 pgTypeId, const NYql::NPg::TTypeDesc& type) {
        auto desc = TPgTypeDescriptor(type);
        ByName[desc.YdbTypeName] = pgTypeId;
        PgTypeDescriptors.emplace(pgTypeId, desc);
    }

private:
    THashMap<ui32, TPgTypeDescriptor> PgTypeDescriptors;
    THashMap<TString, ui32> ByName;
};

ui32 PgTypeIdFromTypeDesc(void* typeDesc) {
    if (!typeDesc) {
        return 0;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->TypeId;
}

void* TypeDescFromPgTypeId(ui32 pgTypeId) {
    if (!pgTypeId) {
        return {};
    }
    return (void*)TPgTypeDescriptors::Instance().Find(pgTypeId);
}

TString PgTypeNameFromTypeDesc(void* typeDesc, const TString& typeMod) {
    if (!typeDesc) {
        return "";
    }
    auto* pgTypeDesc = static_cast<TPgTypeDescriptor*>(typeDesc);
    if (typeMod.empty()) {
        return pgTypeDesc->YdbTypeName;
    }
    return pgTypeDesc->YdbTypeName + INTERNAL_TYPE_AND_MOD_SEPARATOR + typeMod;
}

void* TypeDescFromPgTypeName(const TStringBuf name) {
    auto space = name.find_first_of(INTERNAL_TYPE_AND_MOD_SEPARATOR);
    if (space != TStringBuf::npos) {
        return (void*)TPgTypeDescriptors::Instance().Find(name.substr(0, space));
    }
    return (void*)TPgTypeDescriptors::Instance().Find(name);
}

TString TypeModFromPgTypeName(const TStringBuf name) {
    auto space = name.find_first_of(INTERNAL_TYPE_AND_MOD_SEPARATOR);
    if (space != TStringBuf::npos) {
        return TString(name.substr(space + 1));
    }
    return {};
}

bool TypeDescIsComparable(void* typeDesc) {
    if (!typeDesc) {
        return false;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->CompareProcId != 0;
}

i32 TypeDescGetTypeLen(void* typeDesc) {
    if (!typeDesc) {
        return 0;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->TypeLen;
}

ui32 TypeDescGetStoredSize(void* typeDesc) {
    if (!typeDesc) {
        return 0;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->StoredSize;
}

bool TypeDescNeedsCoercion(void* typeDesc) {
    if (!typeDesc) {
        return false;
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->NeedsCoercion;
}

int PgNativeBinaryCompare(const char* dataL, size_t sizeL, const char* dataR, size_t sizeR, void* typeDesc) {
    return static_cast<TPgTypeDescriptor*>(typeDesc)->Compare(dataL, sizeL, dataR, sizeR);
}

ui64 PgNativeBinaryHash(const char* data, size_t size, void* typeDesc) {
    return static_cast<TPgTypeDescriptor*>(typeDesc)->Hash(data, size);
}

TTypeModResult BinaryTypeModFromTextTypeMod(const TString& str, void* typeDesc) {
    if (!typeDesc) {
        return {-1, "invalid type descriptor"};
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->ReadTypeMod(str);
}

TMaybe<TString> PgNativeBinaryValidate(const TStringBuf binary, void* typeDesc) {
    if (!typeDesc) {
        return "invalid type descriptor";
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->Validate(binary);
}

TCoerceResult PgNativeBinaryCoerce(const TStringBuf binary, void* typeDesc, i32 typmod) {
    if (!typeDesc) {
        return {{}, "invalid type descriptor"};
    }
    return static_cast<TPgTypeDescriptor*>(typeDesc)->Coerce(binary, typmod);
}

TConvertResult PgNativeBinaryFromNativeText(const TString& str, ui32 pgTypeId) {
    auto* typeDesc = TypeDescFromPgTypeId(pgTypeId);
    Y_VERIFY(typeDesc);
    return static_cast<TPgTypeDescriptor*>(typeDesc)->NativeBinaryFromNativeText(str);
}

TConvertResult PgNativeTextFromNativeBinary(const TString& binary, ui32 pgTypeId) {
    auto* typeDesc = TypeDescFromPgTypeId(pgTypeId);
    Y_VERIFY(typeDesc);
    return static_cast<TPgTypeDescriptor*>(typeDesc)->NativeTextFromNativeBinary(binary);
}

} // namespace NKikimr::NPg

namespace NYql::NCommon {

TString PgValueCoerce(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId, i32 typMod, TMaybe<TString>* error) {
    auto* typeDesc = NKikimr::NPg::TypeDescFromPgTypeId(pgTypeId);
    if (!typeDesc) {
        if (error) {
            *error = "invalid type descriptor";
        }
        return {};
    }
    auto result = static_cast<NKikimr::NPg::TPgTypeDescriptor*>(typeDesc)->Coerce(value, typMod);
    if (result.Error) {
        if (error) {
            *error = result.Error;
        }
        return {};
    }
    return *result.NewValue;
}

} // namespace NYql::NCommon
