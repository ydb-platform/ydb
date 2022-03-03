#include "comp_factory.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/providers/common/codec/yql_pg_codec.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "thread_inits.h"
#undef Abs
#undef Min
#undef Max
#undef TypeName
#undef SortBy
#undef LOG
#undef INFO
#undef NOTICE
#undef WARNING
#undef ERROR
#undef FATAL
#undef PANIC
#undef open
#undef fopen
#undef bind
#undef locale_t
}

namespace NYql {

using namespace NKikimr::NMiniKQL;

struct TPAllocListItem {
    TPAllocListItem* Next = nullptr;
    TPAllocListItem* Prev = nullptr;
};

static_assert(sizeof(TPAllocListItem) == 16);

Y_POD_THREAD(TPAllocListItem*) PAllocList;

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
    if (PAllocList) {
        // remove this block from list
        auto current = (TPAllocListItem*)original;
        current->Prev->Next = current->Next;
        current->Next->Prev = current->Prev;
    }

    auto raw = (NUdf::IBoxedValue*)original;
    new(raw) TBoxedValueWithFree();
    NUdf::IBoxedValuePtr ref(raw);
    return NUdf::TUnboxedValuePod(std::move(ref));
}

Datum PointerDatumFromPod(const NUdf::TUnboxedValuePod& value) {
    return (Datum)(((const char*)value.AsBoxed().Get()) + PallocHdrSize);
}

struct TPAllocLeakGuard {
    TPAllocLeakGuard() {
        Y_ENSURE(!PAllocList);
        PAllocList = &Root;
        Root.Next = &Root;
        Root.Prev = &Root;
    }

    ~TPAllocLeakGuard() {
        auto current = Root.Next;
        while (current != &Root) {
            auto next = current->Next;
            MKQLFreeDeprecated((void*)current);
            current = next;
        }

        PAllocList = nullptr;
    }

    TPAllocListItem Root;
};

void *MkqlAllocSetAlloc(MemoryContext context, Size size) {
    auto fullSize = size + PallocHdrSize;
    auto ptr = (char *)NKikimr::NMiniKQL::MKQLAllocDeprecated(fullSize);
    auto ret = (void*)(ptr + PallocHdrSize);
    *(MemoryContext *)(((char *)ret) - sizeof(void *)) = context;
    if (PAllocList) {
        // add to linked list
        auto current = (TPAllocListItem*)ptr;
        PAllocList->Prev->Next = current;
        current->Prev = PAllocList->Prev;
        current->Next = PAllocList;
        PAllocList->Prev = current;
    }

    return ret;
}

void MkqlAllocSetFree(MemoryContext context, void* pointer) {
    if (pointer) {
        auto original = (void*)((char*)pointer - PallocHdrSize);
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

struct TMkqlPgAdapter {
    TMkqlPgAdapter() {
        MemoryContextCreate((MemoryContext)&Data,
            T_AllocSetContext,
            &MkqlMethods,
            nullptr,
            "mkql");
    }

    static MemoryContext Instance() {
        return (MemoryContext)&Singleton<TMkqlPgAdapter>()->Data;
    }

    MemoryContextData Data;
};

#define SET_MEMORY_CONTEXT \
    CurrentMemoryContext = ErrorContext = TMkqlPgAdapter::Instance(); \
    Y_DEFER { \
        CurrentMemoryContext = ErrorContext = nullptr; \
    };

class TPgConst : public TMutableComputationNode<TPgConst> {
    typedef TMutableComputationNode<TPgConst> TBaseComputation;
public:
    TPgConst(TComputationMutables& mutables, ui32 typeId, const std::string_view& value)
        : TBaseComputation(mutables)
        , TypeId(typeId)
        , Value(value)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        SET_MEMORY_CONTEXT;

        if (TypeId == INT4OID) {
            return ScalarDatumToPod(Int32GetDatum(FromString<i32>(Value)));
        } else if (TypeId == FLOAT8OID) {
            return ScalarDatumToPod(Float8GetDatum(FromString<double>(Value)));
        } else if (TypeId == TEXTOID) {
            return PointerDatumToPod(PointerGetDatum(cstring_to_text_with_len(Value.data(), Value.size())));
        } else if (TypeId == BOOLOID) {
            return ScalarDatumToPod(BoolGetDatum(!Value.empty() && Value[0] == 't'));
        } else {
            UdfTerminate((TStringBuilder() << "Unsupported pg type id:" << TypeId).c_str());
        }
    }

private:
    void RegisterDependencies() const final {
    }

    const ui32 TypeId;
    const std::string_view Value;
};

class TFunctionCallInfo {
public:
    TFunctionCallInfo(ui32 numArgs, const FmgrInfo* finfo)
        : NumArgs(numArgs)
    {
        if (!finfo->fn_addr) {
            return;
        }

        MemSize = SizeForFunctionCallInfo(numArgs);
        Ptr = MKQLAllocWithSize(MemSize);
        auto& callInfo = Ref();
        Zero(callInfo);
        callInfo.flinfo = const_cast<FmgrInfo*>(finfo);
        callInfo.nargs = NumArgs;
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
};

class TPgResolvedCall : public TMutableComputationNode<TPgResolvedCall> {
    typedef TMutableComputationNode<TPgResolvedCall> TBaseComputation;
public:
    TPgResolvedCall(TComputationMutables& mutables, const std::string_view& name, ui32 id, TComputationNodePtrVector&& argNodes)
        : TBaseComputation(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , Name(name)
        , Id(id)
        , ArgNodes(std::move(argNodes))
        , ProcDesc(NPg::LookupProc(id))
        , RetTypeDesc(NPg::LookupType(ProcDesc.ResultType))
    {
        Zero(FInfo);
        fmgr_info(Id, &FInfo);
        Y_ENSURE(!FInfo.fn_retset);
        Y_ENSURE(FInfo.fn_addr);
        Y_ENSURE(FInfo.fn_nargs == ArgNodes.size());
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto& state = GetState(compCtx);
        auto& callInfo = state.CallInfo.Ref();
        callInfo.isnull = false;
        for (ui32 i = 0; i < ArgNodes.size(); ++i) {
            auto value = ArgNodes[i]->GetValue(compCtx);
            NullableDatum argDatum = { 0, false };
            if (!value) {
                if (FInfo.fn_strict) {
                    return NUdf::TUnboxedValuePod();
                }

                argDatum.isnull = true;
            } else {
                argDatum.value = value.IsBoxed() ? PointerDatumFromPod(value) : ScalarDatumFromPod(value);
            }

            callInfo.args[i] = argDatum;
        }

        SET_MEMORY_CONTEXT;
        TPAllocLeakGuard leakGuard;
        PG_TRY();
        {
            auto ret = FInfo.fn_addr(&callInfo);
            if (callInfo.isnull) {
                return NUdf::TUnboxedValuePod();
            }

            return RetTypeDesc.PassByValue ? ScalarDatumToPod(ret) : PointerDatumToPod(ret);
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

private:
    void RegisterDependencies() const final {
        for (const auto node : ArgNodes) {
            DependsOn(node);
        }
    }

    struct TState : public TComputationValue<TState> {
        TState(TMemoryUsageInfo* memInfo, ui32 numArgs, const FmgrInfo* finfo)
            : TComputationValue(memInfo)
            , CallInfo(numArgs, finfo)
        {
        }

        TFunctionCallInfo CallInfo;
    };

    TState& GetState(TComputationContext& compCtx) const {
        auto& result = compCtx.MutableValues[StateIndex];
        if (!result.HasValue()) {
            result = compCtx.HolderFactory.Create<TState>(ArgNodes.size(), &FInfo);
        }

        return *static_cast<TState*>(result.AsBoxed().Get());
    }

    const ui32 StateIndex;
    const std::string_view Name;
    const ui32 Id;
    FmgrInfo FInfo;
    const NPg::TProcDesc ProcDesc;
    const NPg::TTypeDesc RetTypeDesc;
    const TComputationNodePtrVector ArgNodes;
};

class TPgCast : public TMutableComputationNode<TPgCast> {
    typedef TMutableComputationNode<TPgCast> TBaseComputation;
public:
    TPgCast(TComputationMutables& mutables, ui32 sourceId, ui32 targetId, IComputationNode* arg)
        : TBaseComputation(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , SourceId(sourceId)
        , TargetId(targetId)
        , Arg(arg)
        , TargetTypeDesc(NPg::LookupType(targetId))
    {
        Zero(FInfo1);
        Zero(FInfo2);
        if (SourceId == 0 || SourceId == TargetId) {
            return;
        }

        const auto& sourceTypeDesc = NPg::LookupType(SourceId);
        ui32 funcId;
        ui32 funcId2 = 0;
        if (!NPg::HasCast(SourceId, TargetId)) {
            if (sourceTypeDesc.Category == 'S') {
                funcId = TargetTypeDesc.InFuncId;
            } else {
                Y_ENSURE(TargetTypeDesc.Category == 'S');
                funcId = sourceTypeDesc.OutFuncId;
            }
        } else {
            const auto& cast = NPg::LookupCast(SourceId, TargetId);
            switch (cast.Method) {
                case NPg::ECastMethod::Binary:
                    return;
                case NPg::ECastMethod::Function: {
                    Y_ENSURE(cast.FunctionId);
                    funcId = cast.FunctionId;
                    break;
                }
                case NPg::ECastMethod::InOut: {
                    funcId = sourceTypeDesc.OutFuncId;
                    funcId2 = TargetTypeDesc.InFuncId;
                    break;
                }
            }
        }

        fmgr_info(funcId, &FInfo1);
        Y_ENSURE(!FInfo1.fn_retset);
        Y_ENSURE(FInfo1.fn_addr);
        Y_ENSURE(FInfo1.fn_nargs == 1);
        Func1Lookup = NPg::LookupProc(funcId);
        Y_ENSURE(Func1Lookup.ArgTypes.size() == 1);
        if (Func1Lookup.ArgTypes[0] == CSTRINGOID && sourceTypeDesc.Category == 'S') {
            ConvertArgToCString = true;
        }

        if (funcId2) {
            fmgr_info(funcId2, &FInfo2);
            Y_ENSURE(!FInfo2.fn_retset);
            Y_ENSURE(FInfo2.fn_addr);
            Y_ENSURE(FInfo2.fn_nargs == 1);
            Func2Lookup = NPg::LookupProc(funcId2);
            Y_ENSURE(Func2Lookup.ArgTypes.size() == 1);
        }

        if (!funcId2) {
            if (Func1Lookup.ResultType == CSTRINGOID && TargetTypeDesc.Category == 'S') {
                ConvertResFromCString = true;
            }
        } else {
            const auto& Func2ArgType = NPg::LookupType(Func2Lookup.ArgTypes[0]);
            if (Func1Lookup.ResultType == CSTRINGOID && Func2ArgType.Category == 'S') {
                ConvertResFromCString = true;
            }

            if (Func2Lookup.ResultType == CSTRINGOID && TargetTypeDesc.Category == 'S') {
                ConvertResFromCString2 = true;
            }
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto value = Arg->GetValue(compCtx);
        if (!value || !FInfo1.fn_addr) {
            return value.Release();
        }

        auto& state = GetState(compCtx);
        auto& callInfo1 = state.CallInfo1.Ref();
        callInfo1.isnull = false;
        NullableDatum argDatum = { value.IsBoxed() ? PointerDatumFromPod(value) : ScalarDatumFromPod(value), false };
        SET_MEMORY_CONTEXT;
        TPAllocLeakGuard leakGuard;
        if (ConvertArgToCString) {
            argDatum.value = (Datum)text_to_cstring((const text*)argDatum.value);
            Y_DEFER {
                pfree((void*)argDatum.value);
            };
        }

        callInfo1.args[0] = argDatum;

        PG_TRY();
        {
            auto ret = FInfo1.fn_addr(&callInfo1);
            if (callInfo1.isnull) {
                return NUdf::TUnboxedValuePod();
            }

            void* freeMem = nullptr;
            if (ConvertResFromCString) {
                freeMem = (void*)ret;
                ret = (Datum)cstring_to_text((const char*)ret);

                Y_DEFER {
                    pfree(freeMem);
                };
            }

            if (FInfo2.fn_addr) {
                auto& callInfo2 = state.CallInfo1.Ref();
                callInfo2.isnull = false;
                NullableDatum argDatum2 = { ret, false };
                callInfo2.args[0] = argDatum2;

                auto ret2 = FInfo2.fn_addr(&callInfo2);
                pfree((void*)ret);

                if (callInfo2.isnull) {
                    return NUdf::TUnboxedValuePod();
                }

                ret = ret2;
            }

            void* freeMem2 = nullptr;
            if (ConvertResFromCString2) {
                freeMem2 = (void*)ret;
                ret = (Datum)cstring_to_text((const char*)ret);

                Y_DEFER{
                    pfree(freeMem2);
                };
            }

            return TargetTypeDesc.PassByValue ? ScalarDatumToPod(ret) : PointerDatumToPod(ret);
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

private:
    void RegisterDependencies() const final {
        DependsOn(Arg);
    }

    struct TState : public TComputationValue<TState> {
        TState(TMemoryUsageInfo* memInfo, const FmgrInfo* finfo1, const FmgrInfo* finfo2)
            : TComputationValue(memInfo)
            , CallInfo1(1, finfo1)
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


    const ui32 StateIndex;
    const ui32 SourceId;
    const ui32 TargetId;
    IComputationNode* const Arg;
    const NPg::TTypeDesc TargetTypeDesc;
    FmgrInfo FInfo1, FInfo2;
    NPg::TProcDesc Func1Lookup, Func2Lookup;
    bool ConvertArgToCString = false;
    bool ConvertResFromCString = false;
    bool ConvertResFromCString2 = false;
};

TComputationNodeFactory GetPgFactory() {
    return [] (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            pg_thread_init();
            TStringBuf name = callable.GetType()->GetName();
            if (name == "PgConst") {
                const auto typeIdData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto valueData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                ui32 typeId = typeIdData->AsValue().Get<ui32>();
                auto value = valueData->AsValue().AsStringRef();
                return new TPgConst(ctx.Mutables, typeId, value);
            }

            if (name == "PgResolvedCall") {
                const auto nameData = AS_VALUE(TDataLiteral, callable.GetInput(0));
                const auto idData = AS_VALUE(TDataLiteral, callable.GetInput(1));
                auto name = nameData->AsValue().AsStringRef();
                ui32 id = idData->AsValue().Get<ui32>();
                TComputationNodePtrVector argNodes;
                for (ui32 i = 2; i < callable.GetInputsCount(); ++i) {
                    argNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
                }

                return new TPgResolvedCall(ctx.Mutables, name, id, std::move(argNodes));
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
                return new TPgCast(ctx.Mutables, sourceId, targetId, arg);
            }

            return nullptr;
        };
}

namespace NCommon {

void WriteYsonValuePg(TYsonResultWriter& writer, const NUdf::TUnboxedValuePod& value, NKikimr::NMiniKQL::TPgType* type,
    const TVector<ui32>* structPositions) {
    if (!value) {
        writer.OnNull();
        return;
    }

    TString ret;
    switch (type->GetTypeId()) {
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
    case FLOAT8OID:
        ret = ::FloatToString(DatumGetFloat8(ScalarDatumFromPod(value)));
        break;
    default:
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        if (typeInfo.TypeLen == -1) {
            auto datum = (const text *)PointerDatumFromPod(value);
            ui32 len = VARSIZE_ANY_EXHDR(datum);
            if (len) {
                ret = TString::Uninitialized(len);
                text_to_cstring_buffer(datum, ret.begin(), len + 1);
            }
        } else if (typeInfo.TypeLen == -2) {
            auto str = (const char*)PointerDatumFromPod(value);
            ret = str;
        } else {
            throw yexception() << "Unsupported pg type: " << type->GetName();
        }
    }

    writer.OnStringScalar(ret);
}

} // namespace NCommon
} // NYql

