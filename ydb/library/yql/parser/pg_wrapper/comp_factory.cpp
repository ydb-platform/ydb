#include "comp_factory.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/providers/common/codec/yql_pg_codec.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/core/yql_pg_utils.h>
#include <library/cpp/yson/detail.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "lib/stringinfo.h"
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

Datum PointerDatumFromPod(const NUdf::TUnboxedValuePod& value, bool isVar) {
    if (value.IsBoxed()) {
        return (Datum)(((const char*)value.AsBoxed().Get()) + PallocHdrSize);
    }

    // temporary palloc, should be handled by TPAllocScope
    const auto& ref = value.AsStringRef();
    if (isVar) {
        return (Datum)cstring_to_text_with_len(ref.Data(), ref.Size());
    } else {
        auto ret = (char*)palloc(ref.Size() + 1);
        memcpy(ret, ref.Data(), ref.Size());
        ret[ref.Size()] = '\0';
        return (Datum)ret;
    }
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
    CurrentMemoryContext = ErrorContext = TMkqlPgAdapter::Instance();

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

        if (TypeId == INT2OID) {
            return ScalarDatumToPod(Int16GetDatum(FromString<i16>(Value)));
        } else if (TypeId == INT4OID) {
            return ScalarDatumToPod(Int32GetDatum(FromString<i32>(Value)));
        } else if (TypeId == INT8OID) {
            return ScalarDatumToPod(Int64GetDatum(FromString<i64>(Value)));
        } else if (TypeId == FLOAT4OID) {
            return ScalarDatumToPod(Float4GetDatum(FromString<float>(Value)));
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
        Y_ENSURE(Id);
        fmgr_info(Id, &FInfo);
        Y_ENSURE(!FInfo.fn_retset);
        Y_ENSURE(FInfo.fn_addr);
        Y_ENSURE(FInfo.fn_nargs == ArgNodes.size());
        ArgDesc.reserve(ProcDesc.ArgTypes.size());
        for (const auto& x : ProcDesc.ArgTypes) {
            ArgDesc.emplace_back(NPg::LookupType(x));
        }

        Y_ENSURE(ArgDesc.size() == ArgNodes.size());
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        SET_MEMORY_CONTEXT;
        TPAllocScope inputArgs;

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
                argDatum.value = ArgDesc[i].PassByValue ?
                    ScalarDatumFromPod(value) :
                    PointerDatumFromPod(value, ArgDesc[i].TypeLen == -1);
            }

            callInfo.args[i] = argDatum;
        }

        inputArgs.Detach();
        PG_TRY();
        {
            TPAllocScope call;
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
    TVector<NPg::TTypeDesc> ArgDesc;
};

inline ui32 MakeTypeIOParam(const NPg::TTypeDesc& desc) {
    return desc.ElementTypeId ? desc.ElementTypeId : desc.TypeId;
}

class TPgCast : public TMutableComputationNode<TPgCast> {
    typedef TMutableComputationNode<TPgCast> TBaseComputation;
public:
    TPgCast(TComputationMutables& mutables, ui32 sourceId, ui32 targetId, IComputationNode* arg)
        : TBaseComputation(mutables)
        , StateIndex(mutables.CurValueIndex++)
        , SourceId(sourceId)
        , TargetId(targetId)
        , Arg(arg)
        , SourceTypeDesc(SourceId ? NPg::LookupType(SourceId) : NPg::TTypeDesc())
        , TargetTypeDesc(NPg::LookupType(targetId))
    {
        TypeIOParam = MakeTypeIOParam(TargetTypeDesc);

        Zero(FInfo1);
        Zero(FInfo2);
        if (SourceId == 0 || SourceId == TargetId) {
            return;
        }

        ui32 funcId;
        ui32 funcId2 = 0;
        if (!NPg::HasCast(SourceId, TargetId)) {
            if (SourceTypeDesc.Category == 'S') {
                funcId = TargetTypeDesc.InFuncId;
            } else {
                Y_ENSURE(TargetTypeDesc.Category == 'S');
                funcId = SourceTypeDesc.OutFuncId;
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
                    funcId = SourceTypeDesc.OutFuncId;
                    funcId2 = TargetTypeDesc.InFuncId;
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
        if (Func1Lookup.ArgTypes[0] == CSTRINGOID && SourceTypeDesc.Category == 'S') {
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

        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        auto& state = GetState(compCtx);
        auto& callInfo1 = state.CallInfo1.Ref();
        callInfo1.isnull = false;
        NullableDatum argDatum = { SourceTypeDesc.PassByValue ?
            ScalarDatumFromPod(value) :
            PointerDatumFromPod(value, SourceTypeDesc.TypeLen == -1), false };
        if (ConvertArgToCString) {
            argDatum.value = (Datum)text_to_cstring((const text*)argDatum.value);
            Y_DEFER {
                pfree((void*)argDatum.value);
            };
        }

        callInfo1.args[0] = argDatum;
        callInfo1.args[1] = { ObjectIdGetDatum(TypeIOParam), false };
        callInfo1.args[2] = { Int32GetDatum(-1), false };

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


    const ui32 StateIndex;
    const ui32 SourceId;
    const ui32 TargetId;
    IComputationNode* const Arg;
    const NPg::TTypeDesc SourceTypeDesc;
    const NPg::TTypeDesc TargetTypeDesc;
    FmgrInfo FInfo1, FInfo2;
    NPg::TProcDesc Func1Lookup, Func2Lookup;
    bool ConvertArgToCString = false;
    bool ConvertResFromCString = false;
    bool ConvertResFromCString2 = false;
    ui32 TypeIOParam = 0;
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
            return NUdf::TUnboxedValuePod(DatumGetBool(ScalarDatumFromPod(value)));
        case NUdf::EDataSlot::Int16:
            return NUdf::TUnboxedValuePod(DatumGetInt16(ScalarDatumFromPod(value)));
        case NUdf::EDataSlot::Int32:
            return NUdf::TUnboxedValuePod(DatumGetInt32(ScalarDatumFromPod(value)));
        case NUdf::EDataSlot::Int64:
            return NUdf::TUnboxedValuePod(DatumGetInt64(ScalarDatumFromPod(value)));
        case NUdf::EDataSlot::Float:
            return NUdf::TUnboxedValuePod(DatumGetFloat4(ScalarDatumFromPod(value)));
        case NUdf::EDataSlot::Double:
            return NUdf::TUnboxedValuePod(DatumGetFloat8(ScalarDatumFromPod(value)));
        case NUdf::EDataSlot::String:
        case NUdf::EDataSlot::Utf8:
            if (value.IsEmbedded() || value.IsString()) {
                return value.Release();
            }

            if (IsCString) {
                auto x = (const char*)value.AsBoxed().Get() + PallocHdrSize;
                return MakeString(TStringBuf(x));
            } else {
                auto x = (const text*)((const char*)value.AsBoxed().Get() + PallocHdrSize);
                ui32 len = VARSIZE_ANY_EXHDR(x);
                TString s;
                if (len) {
                    s = TString::Uninitialized(len);
                    text_to_cstring_buffer(x, s.begin(), len + 1);
                }

                return MakeString(s);
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
            SET_MEMORY_CONTEXT;
            return PointerDatumToPod((Datum)cstring_to_text_with_len(ref.Data(), ref.Size()));
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
                    if (inputType->IsData() || inputType->IsOptional()) {
                        bool isOptional;
                        sourceId = *ConvertToPgType(*UnpackOptionalData(inputType, isOptional)->GetDataSlot());
                    } else {
                        sourceId = AS_TYPE(TPgType, inputType)->GetTypeId();
                    }
                }

                auto returnType = callable.GetType()->GetReturnType();
                auto targetId = AS_TYPE(TPgType, returnType)->GetTypeId();
                return new TPgCast(ctx.Mutables, sourceId, targetId, arg);
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
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto x = (const text*)PointerDatumFromPod(value, true);
        ui32 len = VARSIZE_ANY_EXHDR(x);
        TString s;
        if (len) {
            s = TString::Uninitialized(len);
            text_to_cstring_buffer(x, s.begin(), len + 1);
        }

        buf.Write(StringMarker);
        buf.WriteVarI32(len);
        buf.WriteMany(s);
        break;
    }
    case CSTRINGOID: {
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        auto s = (const char*)PointerDatumFromPod(value, false);
        auto len = strlen(s);
        buf.Write(StringMarker);
        buf.WriteVarI32(len);
        buf.WriteMany(s, len);
        break;
    }
    default:
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(typeInfo.SendFuncId);
        fmgr_info(typeInfo.SendFuncId, &finfo);
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
            PointerDatumFromPod(value, typeInfo.TypeLen == -1), false };
        auto x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER {
            pfree(x);
        };

        ui32 len = VARSIZE_ANY_EXHDR(x);
        TString s;
        if (len) {
            s = TString::Uninitialized(len);
            text_to_cstring_buffer(x, s.begin(), len + 1);
        }

        buf.Write(StringMarker);
        buf.WriteVarI32(len);
        buf.WriteMany(s);
        break;
    }
}

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
    case FLOAT4OID:
        ret = ::FloatToString(DatumGetFloat4(ScalarDatumFromPod(value)));
        break;
    case FLOAT8OID:
        ret = ::FloatToString(DatumGetFloat8(ScalarDatumFromPod(value)));
        break;
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto x = (const text*)PointerDatumFromPod(value, true);
        ui32 len = VARSIZE_ANY_EXHDR(x);
        if (len) {
            ret = TString::Uninitialized(len);
            text_to_cstring_buffer(x, ret.begin(), len + 1);
        }
        break;
    }
    case CSTRINGOID: {
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        auto str = (const char*)PointerDatumFromPod(value, false);
        ret = str;
        break;
    }
    default:
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(typeInfo.OutFuncId);
        fmgr_info(typeInfo.OutFuncId, &finfo);
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
            PointerDatumFromPod(value, typeInfo.TypeLen == -1), false };
        auto str = (char*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER {
            pfree(str);
        };

        ret = str;
    }

    writer.OnStringScalar(ret);
}

NUdf::TUnboxedValue ReadYsonValuePg(TPgType* type, char cmd, TInputBuf& buf) {
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
        SET_MEMORY_CONTEXT;
        auto ret = cstring_to_text_with_len(s.Data(), s.Size());
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        CHECK_EXPECTED(cmd, StringMarker);
        auto s = buf.ReadYtString();
        SET_MEMORY_CONTEXT;
        auto ret = (char*)palloc(s.Size() + 1);
        memcpy(ret, s.Data(), s.Size());
        ret[s.Size()] = '\0';
        return PointerDatumToPod((Datum)ret);
    }
    default:
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        auto s = buf.ReadYtString();
        StringInfoData stringInfo;
        stringInfo.data = (char*)s.Data();
        stringInfo.len = s.Size();
        stringInfo.maxlen = s.Size();
        stringInfo.cursor = 0;

        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto typeIOParam = MakeTypeIOParam(typeInfo);
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(typeInfo.ReceiveFuncId);
        fmgr_info(typeInfo.ReceiveFuncId, &finfo);
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
        TString s;
        if (size) {
            s = TString::Uninitialized(size);
            buf.ReadMany(s.begin(), size);
        }

        SET_MEMORY_CONTEXT;
        auto ret = cstring_to_text_with_len(s.Data(), s.Size());
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        TString s;
        if (size) {
            s = TString::Uninitialized(size);
            buf.ReadMany(s.begin(), size);
        }

        SET_MEMORY_CONTEXT;
        auto ret = (char*)palloc(s.Size() + 1);
        memcpy(ret, s.Data(), s.Size());
        ret[s.Size()] = '\0';
        return PointerDatumToPod((Datum)ret);
    }
    default:
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        ui32 size;
        buf.ReadMany((char*)&size, sizeof(size));
        CHECK_STRING_LENGTH_UNSIGNED(size);
        TString s;
        if (size) {
            s = TString::Uninitialized(size);
            buf.ReadMany(s.begin(), size);
        }

        StringInfoData stringInfo;
        stringInfo.data = (char*)s.Data();
        stringInfo.len = s.Size();
        stringInfo.maxlen = s.Size();
        stringInfo.cursor = 0;

        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        auto typeIOParam = MakeTypeIOParam(typeInfo);
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(typeInfo.ReceiveFuncId);
        fmgr_info(typeInfo.ReceiveFuncId, &finfo);
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
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto x = (const text*)PointerDatumFromPod(value, true);
        ui32 len = VARSIZE_ANY_EXHDR(x);
        buf.WriteMany((const char*)&len, sizeof(len));
        TString s;
        if (len) {
            s = TString::Uninitialized(len);
            text_to_cstring_buffer(x, s.begin(), len + 1);
        }

        buf.WriteMany(s.Data(), s.size());
        break;
    }
    case CSTRINGOID: {
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto x = (const char*)PointerDatumFromPod(value, false);
        ui32 len = strlen(x);
        buf.WriteMany((const char*)&len, sizeof(len));
        buf.WriteMany(x, len);
        break;
    }
    default:
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(typeInfo.SendFuncId);
        fmgr_info(typeInfo.SendFuncId, &finfo);
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
            PointerDatumFromPod(value, typeInfo.TypeLen == -1), false };
        auto x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER{
            pfree(x);
        };

        ui32 len = VARSIZE_ANY_EXHDR(x);
        buf.WriteMany((const char*)&len, sizeof(len));
        TString s;
        if (len) {
            s = TString::Uninitialized(len);
            text_to_cstring_buffer(x, s.begin(), len + 1);
        }

        buf.WriteMany(s.Data(), s.size());
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

} // NYql

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql;

void PGPackImpl(const TPgType* type, const NUdf::TUnboxedValuePod& value, TBuffer& buf) {
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
        const auto x = DatumGetFloat4(ScalarDatumFromPod(value));
        NDetails::PutRawData(x, buf);
        break;
    }
    case FLOAT8OID: {
        const auto x = DatumGetFloat8(ScalarDatumFromPod(value));
        NDetails::PutRawData(x, buf);
        break;
    }
    case BYTEAOID:
    case VARCHAROID:
    case TEXTOID: {
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto x = (const text*)PointerDatumFromPod(value, true);
        ui32 len = VARSIZE_ANY_EXHDR(x);
        NDetails::PackUInt32(len, buf);
        auto off = buf.Size();
        buf.Advance(len + 1);
        text_to_cstring_buffer(x, buf.Data() + off, len + 1);
        buf.EraseBack(1);
        break;
    }
    case CSTRINGOID: {
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto x = (const char*)PointerDatumFromPod(value, false);
        const auto len = strlen(x);
        NDetails::PackUInt32(len, buf);
        buf.Append(x, len);
        break;
    }
    default:
        SET_MEMORY_CONTEXT;
        TPAllocScope call;
        const auto& typeInfo = NPg::LookupType(type->GetTypeId());
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(typeInfo.SendFuncId);
        fmgr_info(typeInfo.SendFuncId, &finfo);
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
            PointerDatumFromPod(value, typeInfo.TypeLen == -1), false };
        auto x = (text*)finfo.fn_addr(callInfo);
        Y_ENSURE(!callInfo->isnull);
        Y_DEFER{
            pfree(x);
        };

        ui32 len = VARSIZE_ANY_EXHDR(x);
        NDetails::PackUInt32(len, buf);
        auto off = buf.Size();
        buf.Advance(len + 1);
        text_to_cstring_buffer(x, buf.Data() + off, len + 1);
        buf.EraseBack(1);
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
        SET_MEMORY_CONTEXT;
        auto size = NDetails::UnpackUInt32(buf);
        MKQL_ENSURE(size <= buf.size(), "Bad packed data. Buffer too small");
        const char* ptr = buf.data();
        buf.Skip(size);
        auto ret = cstring_to_text_with_len(ptr, size);
        return PointerDatumToPod((Datum)ret);
    }
    case CSTRINGOID: {
        SET_MEMORY_CONTEXT;
        auto size = NDetails::UnpackUInt32(buf);
        MKQL_ENSURE(size <= buf.size(), "Bad packed data. Buffer too small");
        const char* ptr = buf.data();
        buf.Skip(size);
        auto ret = (char*)palloc(size + 1);
        memcpy(ret, ptr, size);
        ret[size] = '\0';
        return PointerDatumToPod((Datum)ret);
    }
    default:
        SET_MEMORY_CONTEXT;
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
        FmgrInfo finfo;
        Zero(finfo);
        Y_ENSURE(typeInfo.ReceiveFuncId);
        fmgr_info(typeInfo.ReceiveFuncId, &finfo);
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

} // namespace NMiniKQL
} // namespace NKikimr

