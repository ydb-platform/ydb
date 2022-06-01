#include "comp_factory.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/computation/presort_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/providers/common/codec/yql_pg_codec.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/core/yql_pg_utils.h>
#include <ydb/library/yql/utils/fp_bits.h>
#include <library/cpp/yson/detail.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#define Sort PG_Sort
#define Unique PG_Unique
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
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

namespace NYql {

using namespace NKikimr::NMiniKQL;

static __thread bool NeedCanonizeFp = false;

struct TMainContext {
    MemoryContextData Data;
    MemoryContext PrevCurrentMemoryContext = nullptr;
    MemoryContext PrevErrorContext = nullptr;
};

ui32 GetFullVarSize(const text* s) {
    return VARSIZE(s);
}

ui32 GetCleanVarSize(const text* s) {
    return VARSIZE(s) - VARHDRSZ;
}

const char* GetVarData(const text* s) {
    return VARDATA(s);
}

TStringBuf GetVarBuf(const text* s) {
    return TStringBuf(GetVarData(s), GetCleanVarSize(s));
}

char* GetMutableVarData(text* s) {
    return VARDATA(s);
}

void UpdateCleanVarSize(text* s, ui32 cleanSize) {
    SET_VARSIZE(s, cleanSize + VARHDRSZ);
}

char* MakeCString(TStringBuf s) {
    char* ret = (char*)palloc(s.Size() + 1);
    memcpy(ret, s.Data(), s.Size());
    ret[s.Size()] = '\0';
    return ret;
}

text* MakeVar(TStringBuf s) {
    text* ret = (text*)palloc(s.Size() + VARHDRSZ);
    UpdateCleanVarSize(ret, s.Size());
    memcpy(GetMutableVarData(ret), s.Data(), s.Size());
    return ret;
}

// allow to construct TListEntry in the space for IBoxedValue
static_assert(sizeof(NUdf::IBoxedValue) >= sizeof(TAllocState::TListEntry));

constexpr size_t PallocHdrSize = sizeof(void*) + sizeof(NUdf::IBoxedValue);

NUdf::TUnboxedValuePod ScalarDatumToPod(Datum datum) {
    return NUdf::TUnboxedValuePod((ui64)datum);
}

Datum ScalarDatumFromPod(const NUdf::TUnboxedValuePod& value) {
    return (Datum)value.Get<ui64>();
}

class TBoxedValueWithFree : public NUdf::TBoxedValueBase {
public:
    void operator delete(void *mem) noexcept {
        return MKQLFreeDeprecated(mem);
    }
};

NUdf::TUnboxedValuePod PointerDatumToPod(Datum datum) {
    auto original = (char*)datum - PallocHdrSize;
    // remove this block from list
    ((TAllocState::TListEntry*)original)->Unlink();

    auto raw = (NUdf::IBoxedValue*)original;
    new(raw) TBoxedValueWithFree();
    NUdf::IBoxedValuePtr ref(raw);
    return NUdf::TUnboxedValuePod(std::move(ref));
}

NUdf::TUnboxedValuePod OwnedPointerDatumToPod(Datum datum) {
    auto original = (char*)datum - PallocHdrSize;
    auto raw = (NUdf::IBoxedValue*)original;
    NUdf::IBoxedValuePtr ref(raw);
    return NUdf::TUnboxedValuePod(std::move(ref));
}

Datum PointerDatumFromPod(const NUdf::TUnboxedValuePod& value) {
    return (Datum)(((const char*)value.AsBoxed().Get()) + PallocHdrSize);
}

void *MkqlAllocSetAlloc(MemoryContext context, Size size) {
    auto fullSize = size + PallocHdrSize;
    auto ptr = (char *)NKikimr::NMiniKQL::MKQLAllocDeprecated(fullSize);
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

class TVPtrHolder {
public:
    TVPtrHolder() {
        new(Dummy) TBoxedValueWithFree();
    }

    static bool IsBoxedVPtr(Datum ptr) {
        return *(const uintptr_t*)((char*)ptr - PallocHdrSize) == *(const uintptr_t*)Instance.Dummy;
    }

private:
    char Dummy[sizeof(NUdf::IBoxedValue)];

    static TVPtrHolder Instance;
};

TVPtrHolder TVPtrHolder::Instance;

inline ui32 MakeTypeIOParam(const NPg::TTypeDesc& desc) {
    return desc.ElementTypeId ? desc.ElementTypeId : desc.TypeId;
}

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
        PG_TRY();
        {
            auto ret = FInfo.fn_addr(callInfo);
            Y_ENSURE(!callInfo->isnull);
            return TypeDesc.PassByValue ? ScalarDatumToPod(ret) : PointerDatumToPod(ret);
        }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error in function: " << NPg::LookupProc(TypeDesc.InFuncId).Name << ", reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            UdfTerminate(errMsg.c_str());
        }
        PG_END_TRY();
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
        Ptr = MKQLAllocWithSize(MemSize);
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
            MKQLFreeWithSize(Ptr, MemSize);
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
        Ptr = MKQLAllocWithSize(sizeof(ReturnSetInfo));
        Zero(Ref());
        Ref().type = T_ReturnSetInfo;
    }

    ~TReturnSetInfo() {
        MKQLFreeWithSize(Ptr, sizeof(ReturnSetInfo));
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
        , ArgNodes(std::move(argNodes))
        , ArgTypes(std::move(argTypes))
        , ProcDesc(NPg::LookupProc(id))
        , RetTypeDesc(NPg::LookupType(ProcDesc.ResultType))
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
        PG_TRY();
        {
            auto ret = this->FInfo.fn_addr(&callInfo);
            if (callInfo.isnull) {
                return NUdf::TUnboxedValuePod();
            }

            if (this->RetTypeDesc.PassByValue) {
                return ScalarDatumToPod(ret);
            }

            if (TVPtrHolder::IsBoxedVPtr(ret)) {
                // returned one of arguments
                return OwnedPointerDatumToPod(ret);
            }

            return PointerDatumToPod(ret);
        }
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error in function: " << this->Name << ", reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            UdfTerminate(errMsg.c_str());
        }
        PG_END_TRY();
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
                PG_TRY();
                {
                    callInfo.isnull = false;
                    auto ret = callInfo.flinfo->fn_addr(&callInfo);
                    if (RSInfo.Ref().isDone == ExprEndResult) {
                        IsFinished = true;
                        return false;
                    }

                    if (callInfo.isnull) {
                        value = NUdf::TUnboxedValuePod();
                    } else if (RetTypeDesc.PassByValue) {
                        value = ScalarDatumToPod(ret);
                    } else if (TVPtrHolder::IsBoxedVPtr(ret)) {
                        // returned one of arguments
                        value = OwnedPointerDatumToPod(ret);
                    } else {
                        value = PointerDatumToPod(ret);
                    }

                    return true;
                }
                PG_CATCH();
                {
                    auto error_data = CopyErrorData();
                    TStringBuilder errMsg;
                    errMsg << "Error in function: " << Name << ", reason: " << error_data->message;
                    FreeErrorData(error_data);
                    FlushErrorState();
                    UdfTerminate(errMsg.c_str());
                }
                PG_END_TRY();
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
        if (SourceId == 0 || SourceId == TargetId) {
            return;
        }

        ui32 funcId;
        ui32 funcId2 = 0;
        if (!NPg::HasCast(SourceElemDesc.TypeId, TargetElemDesc.TypeId)) {
            if (SourceElemDesc.Category == 'S') {
                ArrayCast = IsSourceArray;
                if (!IsTargetArray || IsSourceArray) {
                    funcId = TargetElemDesc.InFuncId;
                } else {
                    funcId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
                }
            } else {
                Y_ENSURE(TargetTypeDesc.Category == 'S');
                ArrayCast = IsTargetArray;
                if (!IsSourceArray || IsTargetArray) {
                    funcId = SourceElemDesc.OutFuncId;
                } else {
                    funcId = NPg::LookupProc("array_out", { 0 }).ProcId;
                }
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
        if (Func1Lookup.ArgTypes[0] == CSTRINGOID && SourceElemDesc.Category == 'S') {
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
            if (Func1Lookup.ResultType == CSTRINGOID && TargetElemDesc.Category == 'S') {
                ConvertResFromCString = true;
            }
        } else {
            const auto& Func2ArgType = NPg::LookupType(Func2Lookup.ArgTypes[0]);
            if (Func1Lookup.ResultType == CSTRINGOID && Func2ArgType.Category == 'S') {
                ConvertResFromCString = true;
            }

            if (Func2Lookup.ResultType == CSTRINGOID && TargetElemDesc.Category == 'S') {
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

            Datum* elems = (Datum*)MKQLAllocWithSize(nitems * sizeof(Datum));
            Y_DEFER {
                MKQLFreeWithSize(elems, nitems * sizeof(Datum));
            };

            bool* nulls = (bool*)MKQLAllocWithSize(nitems);
            Y_DEFER{
                MKQLFreeWithSize(nulls, nitems);
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
            return TargetTypeDesc.PassByValue ? ScalarDatumToPod(ret) : PointerDatumToPod(ret);
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
        if (ConvertArgToCString) {
            argDatum.value = (Datum)MakeCString(GetVarBuf((const text*)argDatum.value));
            Y_DEFER{
                pfree((void*)argDatum.value);
            };
        }

        callInfo1.args[0] = argDatum;
        callInfo1.args[1] = { ObjectIdGetDatum(TypeIOParam), false };
        callInfo1.args[2] = { Int32GetDatum(typeMod), false };

        void* freeMem = nullptr;
        void* freeMem2 = nullptr;
        Y_DEFER{
            if (freeMem) {
                pfree(freeMem);
            }

            if (freeMem2) {
                pfree(freeMem2);
            }
        };

        PG_TRY();
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
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error in cast, reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            UdfTerminate(errMsg.c_str());
        }
        PG_END_TRY();
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
};

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

        switch (Slot)
        {
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
            Y_UNREACHABLE();
        }
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

        switch (Slot)
        {
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
            Y_UNREACHABLE();
        }
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

        Datum* dvalues = (Datum*)MKQLAllocWithSize(nelems * sizeof(Datum));
        Y_DEFER {
            MKQLFreeWithSize(dvalues, nelems * sizeof(Datum));
        };

        bool *dnulls = (bool*)MKQLAllocWithSize(nelems);
        Y_DEFER {
            MKQLFreeWithSize(dnulls, nelems);
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

        PG_TRY();
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
        PG_CATCH();
        {
            auto error_data = CopyErrorData();
            TStringBuilder errMsg;
            errMsg << "Error in PgArray, reason: " << error_data->message;
            FreeErrorData(error_data);
            FlushErrorState();
            UdfTerminate(errMsg.c_str());
        }
        PG_END_TRY();
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

            return nullptr;
        };
}

namespace NCommon {

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
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto sendFuncId = typeInfo.SendFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            sendFuncId = NPg::LookupProc("array_send", { 0 }).ProcId;
        }

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
            ScalarDatumFromPod(value):
            PointerDatumFromPod(value), false };
        NeedCanonizeFp = true;
        auto x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER {
            pfree(x);
            NeedCanonizeFp = false;
        };

        auto s = GetVarBuf(x);
        buf.Write(StringMarker);
        buf.WriteVarI32(s.Size());
        buf.WriteMany(s.Data(), s.Size());
        break;
    }
}

TString PgValueToString(const NUdf::TUnboxedValuePod& value, ui32 pgTypeId) {
    if (!value) {
        return "null";
    }

    TString ret;
    switch (pgTypeId) {
    case BOOLOID:
        ret = DatumGetBool(ScalarDatumFromPod(value)) ? "true" : "false";
        break;
    case INT2OID:
        ret = ToString(DatumGetInt16(ScalarDatumFromPod(value)));
        break;
    case INT4OID:
        ret = ToString(DatumGetInt32(ScalarDatumFromPod(value)));
        break;
    case INT8OID:
        ret = ToString(DatumGetInt64(ScalarDatumFromPod(value)));
        break;
    case FLOAT4OID:
        ret = ::FloatToString(DatumGetFloat4(ScalarDatumFromPod(value)));
        break;
    case FLOAT8OID:
        ret = ::FloatToString(DatumGetFloat8(ScalarDatumFromPod(value)));
        break;
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        const auto x = (const text*)PointerDatumFromPod(value);
        ret = GetVarBuf(x);
        break;
    }
    case CSTRINGOID: {
        auto str = (const char*)PointerDatumFromPod(value);
        ret = str;
        break;
    }
    default:
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(pgTypeId);
        auto outFuncId = typeInfo.OutFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            outFuncId = NPg::LookupProc("array_out", { 0 }).ProcId;
        }

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
        auto str = (char*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER{
            pfree(str);
        };

        ret = str;
    }

    return ret;
}

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPgType* type,
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
        StringInfoData stringInfo;
        stringInfo.data = (char*)s.Data();
        stringInfo.len = s.Size();
        stringInfo.maxlen = s.Size();
        stringInfo.cursor = 0;

        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto typeIOParam = MakeTypeIOParam(typeInfo);
        auto receiveFuncId = typeInfo.ReceiveFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            receiveFuncId = NPg::LookupProc("array_recv", { 0,0,0 }).ProcId;
        }

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
        Y_ENSURE(stringInfo.cursor == stringInfo.len);
        return typeInfo.PassByValue ? ScalarDatumToPod(x) : PointerDatumToPod(x);
    }
}

NUdf::TUnboxedValue ReadYsonValuePg(TPgType* type, char cmd, TInputBuf& buf) {
    using namespace NYson::NDetail;
    if (cmd == EntitySymbol) {
        return NUdf::TUnboxedValuePod();
    }

    CHECK_EXPECTED(cmd, StringMarker);
    auto s = buf.ReadYtString();
    switch (type->GetTypeId()) {
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
        TString str{s};

        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto typeIOParam = MakeTypeIOParam(typeInfo);
        auto inFuncId = typeInfo.InFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            inFuncId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
        }

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
        return typeInfo.PassByValue ? ScalarDatumToPod(x) : PointerDatumToPod(x);
    }
}

NKikimr::NUdf::TUnboxedValue ReadSkiffPg(NKikimr::NMiniKQL::TPgType* type, NCommon::TInputBuf& buf) {
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
        char* s = (char*)MKQLAllocWithSize(size);
        Y_DEFER {
            MKQLFreeWithSize(s, size);
        };

        buf.ReadMany(s, size);

        StringInfoData stringInfo;
        stringInfo.data = s;
        stringInfo.len = size;
        stringInfo.maxlen = size;
        stringInfo.cursor = 0;

        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto typeIOParam = MakeTypeIOParam(typeInfo);
        auto receiveFuncId = typeInfo.ReceiveFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            receiveFuncId = NPg::LookupProc("array_recv", { 0,0,0 }).ProcId;
        }

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
        Y_ENSURE(stringInfo.cursor == stringInfo.len);
        return typeInfo.PassByValue ? ScalarDatumToPod(x) : PointerDatumToPod(x);
    }
}

void WriteSkiffPg(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
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
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto sendFuncId = typeInfo.SendFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            sendFuncId = NPg::LookupProc("array_send", { 0 }).ProcId;
        }

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
        NeedCanonizeFp = true;
        auto x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER {
            pfree(x);
            NeedCanonizeFp = false;
        };

        auto s = GetVarBuf(x);
        ui32 len = s.Size();
        buf.WriteMany((const char*)&len, sizeof(len));
        buf.WriteMany(s.Data(), len);
    }
}

extern "C" void ReadSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, NKikimr::NUdf::TUnboxedValue& value, NCommon::TInputBuf& buf) {
    value = ReadSkiffPg(type, buf);
}

extern "C" void WriteSkiffPgValue(NKikimr::NMiniKQL::TPgType* type, const NKikimr::NUdf::TUnboxedValuePod& value, NCommon::TOutputBuf& buf) {
    WriteSkiffPg(type, value, buf);
}

} // namespace NCommon

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
        ret = INTERVAL_MASK(YEAR);
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

} // NYql

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql;

void PGPackImpl(bool stable, const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuffer& buf) {
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
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto sendFuncId = typeInfo.SendFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            sendFuncId = NPg::LookupProc("array_send", { 0 }).ProcId;
        }

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
        NeedCanonizeFp = stable;
        auto x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER{
            pfree(x);
            NeedCanonizeFp = false;
        };

        auto s = GetVarBuf(x);
        NDetails::PackUInt32(s.Size(), buf);
        buf.Append(s.Data(), s.Size());
    }
}

NUdf::TUnboxedValue PGUnpackImpl(const TPgType* type, TStringBuf& buf) {
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
        MKQL_ENSURE(size <= buf.size(), "Bad packed data. Buffer too small");
        const char* ptr = buf.data();
        buf.Skip(size);
        auto ret = MakeVar(TStringBuf(ptr, size));
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        auto size = NDetails::UnpackUInt32(buf);
        MKQL_ENSURE(size <= buf.size(), "Bad packed data. Buffer too small");
        const char* ptr = buf.data();
        buf.Skip(size);
        auto ret = MakeCString(TStringBuf(ptr, size));
        return PointerDatumToPod((Datum)ret);
    }
    default:
        TPAllocScope call;
        auto size = NDetails::UnpackUInt32(buf);
        MKQL_ENSURE(size <= buf.size(), "Bad packed data. Buffer too small");
        StringInfoData stringInfo;
        stringInfo.data = (char*)buf.data();
        stringInfo.len = size;
        stringInfo.maxlen = size;
        stringInfo.cursor = 0;

        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto typeIOParam = MakeTypeIOParam(typeInfo);
        auto receiveFuncId = typeInfo.ReceiveFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            receiveFuncId = NPg::LookupProc("array_recv", { 0,0,0 }).ProcId;
        }

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
        buf.Skip(stringInfo.cursor);
        return typeInfo.PassByValue ? ScalarDatumToPod(x) : PointerDatumToPod(x);
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
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto sendFuncId = typeInfo.SendFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            sendFuncId = NPg::LookupProc("array_send", { 0 }).ProcId;
        }

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
        NeedCanonizeFp = true;
        auto x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER {
            pfree(x);
            NeedCanonizeFp = false;
        };

        auto s = GetVarBuf(x);
        NDetail::EncodeString<false>(output, s);
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

        StringInfoData stringInfo;
        stringInfo.data = (char*)s.Data();
        stringInfo.len = s.Size();
        stringInfo.maxlen = s.Size();
        stringInfo.cursor = 0;

        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto typeIOParam = MakeTypeIOParam(typeInfo);
        auto receiveFuncId = typeInfo.ReceiveFuncId;
        if (typeInfo.TypeId == typeInfo.ArrayTypeId) {
            receiveFuncId = NPg::LookupProc("array_recv", { 0,0,0 }).ProcId;
        }

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
        Y_ENSURE(stringInfo.cursor == stringInfo.len);
        return typeInfo.PassByValue ? ScalarDatumToPod(x) : PointerDatumToPod(x);
    }
}

void* PgInitializeContext(const std::string_view& contextType) {
    if (contextType == "Agg") {
        auto ctx = (AggState*)MKQLAllocWithSize(sizeof(AggState));
        Zero(*ctx);
        *(NodeTag*)ctx = T_AggState;
        ctx->curaggcontext = (ExprContext*)MKQLAllocWithSize(sizeof(ExprContext));
        Zero(*ctx->curaggcontext);
        ctx->curaggcontext->ecxt_per_tuple_memory = (MemoryContext)&((TMainContext*)TlsAllocState->MainContext)->Data;
        return ctx;
    } else if (contextType == "WinAgg") {
        auto ctx = (WindowAggState*)MKQLAllocWithSize(sizeof(WindowAggState));
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
        MKQLFreeWithSize(((AggState*)ctx)->curaggcontext, sizeof(ExprContext));
        MKQLFreeWithSize(ctx, sizeof(AggState));
    } else if (contextType == "WinAgg") {
        MKQLFreeWithSize(ctx, sizeof(WindowAggState));
    } else {
        Y_FAIL("Unsupported context type");
    }
}

class TPgHash : public NUdf::IHash {
public:
    TPgHash(const NMiniKQL::TPgType* type)
        : Type(type)
        , TypeDesc(NPg::LookupType(type->GetTypeId()))
    {
        Y_ENSURE(TypeDesc.HashProcId);

        Zero(FInfoHash);
        fmgr_info(TypeDesc.HashProcId, &FInfoHash);
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

        callInfo->args[0] = { TypeDesc.PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };

        auto x = FInfoHash.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetUInt32(x);
    }

private:
    const NMiniKQL::TPgType* Type;
    const NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoHash;
};

NUdf::IHash::TPtr MakePgHash(const NMiniKQL::TPgType* type) {
    return new TPgHash(type);
}

class TPgCompare : public NUdf::ICompare {
public:
    TPgCompare(const NMiniKQL::TPgType* type)
        : Type(type)
        , TypeDesc(NPg::LookupType(type->GetTypeId()))
    {
        Y_ENSURE(TypeDesc.LessProcId);
        Y_ENSURE(TypeDesc.CompareProcId);

        Zero(FInfoLess);
        fmgr_info(TypeDesc.LessProcId, &FInfoLess);
        Y_ENSURE(!FInfoLess.fn_retset);
        Y_ENSURE(FInfoLess.fn_addr);
        Y_ENSURE(FInfoLess.fn_nargs == 2);

        Zero(FInfoCompare);
        fmgr_info(TypeDesc.CompareProcId, &FInfoCompare);
        Y_ENSURE(!FInfoCompare.fn_retset);
        Y_ENSURE(FInfoCompare.fn_addr);
        Y_ENSURE(FInfoCompare.fn_nargs == 2);
    }

    bool Less(NUdf::TUnboxedValuePod lhs, NUdf::TUnboxedValuePod rhs) const override {
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

        callInfo->args[0] = { TypeDesc.PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { TypeDesc.PassByValue ?
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

        callInfo->args[0] = { TypeDesc.PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { TypeDesc.PassByValue ?
            ScalarDatumFromPod(rhs) :
            PointerDatumFromPod(rhs), false };

        auto x = FInfoCompare.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetInt32(x);
    }

private:
    const NMiniKQL::TPgType* Type;
    const NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoLess, FInfoCompare;
};

NUdf::ICompare::TPtr MakePgCompare(const NMiniKQL::TPgType* type) {
    return new TPgCompare(type);
}

class TPgEquate: public NUdf::IEquate {
public:
    TPgEquate(const NMiniKQL::TPgType* type)
        : Type(type)
        , TypeDesc(NPg::LookupType(type->GetTypeId()))
    {
        Y_ENSURE(TypeDesc.EqualProcId);

        Zero(FInfoEquate);
        fmgr_info(TypeDesc.EqualProcId, &FInfoEquate);
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

        callInfo->args[0] = { TypeDesc.PassByValue ?
            ScalarDatumFromPod(lhs) :
            PointerDatumFromPod(lhs), false };
        callInfo->args[1] = { TypeDesc.PassByValue ?
            ScalarDatumFromPod(rhs) :
            PointerDatumFromPod(rhs), false };

        auto x = FInfoEquate.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        return DatumGetBool(x);
    }

private:
    const NMiniKQL::TPgType* Type;
    const NPg::TTypeDesc TypeDesc;

    FmgrInfo FInfoEquate;
};

NUdf::IEquate::TPtr MakePgEquate(const NMiniKQL::TPgType* type) {
    return new TPgEquate(type);
}

void* PgInitializeMainContext() {
    auto ctx = (TMainContext*)malloc(sizeof(TMainContext));
    MemoryContextCreate((MemoryContext)&ctx->Data,
        T_AllocSetContext,
        &MkqlMethods,
        nullptr,
        "mkql");
    return ctx;
}

void PgDestroyMainContext(void* ctx) {
    free(ctx);
}

void PgAcquireThreadContext(void* ctx) {
    if (ctx) {
        pg_thread_init();
        auto main = (TMainContext*)ctx;
        main->PrevCurrentMemoryContext = CurrentMemoryContext;
        main->PrevErrorContext = ErrorContext;
        CurrentMemoryContext = ErrorContext = (MemoryContext)&main->Data;
    }
}

void PgReleaseThreadContext(void* ctx) {
    if (ctx) {
        auto main = (TMainContext*)ctx;
        CurrentMemoryContext = main->PrevCurrentMemoryContext;
        ErrorContext = main->PrevErrorContext;
    }
}

} // namespace NMiniKQL
} // namespace NKikimr

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

}


