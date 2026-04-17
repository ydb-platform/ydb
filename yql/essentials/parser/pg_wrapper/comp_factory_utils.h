#pragma once

#include "pg_include.h"

#include <yql/essentials/parser/pg_wrapper/pg_include.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/parser/pg_wrapper/utils.h>

#include <util/system/types.h>

#include <memory>
#include <optional>
#include <string_view>

#include <util/generic/ptr.h>

namespace NYql {

class TPgArgsExprBuilder {
public:
    TPgArgsExprBuilder();

    void Add(ui32 argOid);
    Node* Build(const NPg::TProcDesc& procDesc);

private:
    TVector<Param> PgArgNodes;
    std::unique_ptr<List, decltype(&free)> PgFuncArgsList;
    FuncExpr PgFuncNode;
};

class TPgConst {
public:
    TPgConst(ui32 typeId, std::string_view value);

    NUdf::TUnboxedValue ExtractConst(i32 typeMod = -1) const;

    ui32 GetTypeId() const {
        return TypeId;
    }

private:
    const ui32 TypeId;
    const TString Value;
    const NPg::TTypeDesc TypeDesc;
    FmgrInfo FInfo;
    ui32 TypeIOParam;
};

class TPgResolvedCallBase {
public:
    TPgResolvedCallBase(std::string_view name,
                        ui32 id,
                        TVector<NKikimr::NMiniKQL::TType*>&& argTypes,
                        NKikimr::NMiniKQL::TType* returnType,
                        bool isList,
                        const NKikimr::NMiniKQL::TStructType* structType);

    TPgResolvedCallBase(std::string_view name,
                        ui32 id,
                        const TVector<std::optional<ui32>>& argTypeIds,
                        ui32 returnTypeOid,
                        bool isList,
                        const NKikimr::NMiniKQL::TStructType* structType);

    TPgResolvedCallBase(std::string_view name,
                        ui32 id,
                        const TVector<ui32>& argTypeIds,
                        ui32 returnTypeOid);

    size_t GetArgCount() const {
        return ArgDesc.size();
    }
    ui32 GetArgTypeId(size_t index) const {
        return ArgDesc[index].TypeId;
    }
    ui32 GetProcId() const {
        return Id;
    }
    ui32 GetReturnTypeId() const {
        return RetTypeDesc.TypeId;
    }
    const FmgrInfo& GetFInfo() const {
        return FInfo;
    }

protected:
    const std::string_view Name;
    const ui32 Id;
    FmgrInfo FInfo;
    const NPg::TProcDesc ProcDesc;
    const NPg::TTypeDesc RetTypeDesc;
    TVector<NPg::TTypeDesc> ArgDesc;
    TVector<NPg::TTypeDesc> StructTypeDesc;

    TPgArgsExprBuilder ArgsExprBuilder;
};

class TFunctionCallInfo {
public:
    TFunctionCallInfo(ui32 numArgs, const FmgrInfo* finfo);
    ~TFunctionCallInfo();

    TFunctionCallInfo(const TFunctionCallInfo&) = delete;
    TFunctionCallInfo& operator=(const TFunctionCallInfo&) = delete;

    FunctionCallInfoBaseData& Ref();

private:
    const ui32 NumArgs = 0;
    ui32 MemSize = 0;
    void* Ptr = nullptr;
    FmgrInfo CopyFmgrInfo;
};

struct TPgResolvedCallState {
    TPgResolvedCallState(ui32 numArgs, const FmgrInfo* finfo);

    TFunctionCallInfo CallInfo;
    NKikimr::NMiniKQL::TUnboxedValueVector Args;
};

template <bool UseContext>
class TPgResolvedCall: public TPgResolvedCallBase {
public:
    TPgResolvedCall(const std::string_view& name,
                    ui32 id,
                    TVector<NKikimr::NMiniKQL::TType*>&& argTypes,
                    NKikimr::NMiniKQL::TType* returnType);

    TPgResolvedCall(const std::string_view& name,
                    ui32 id,
                    const TVector<std::optional<ui32>>& argTypeIds,
                    ui32 returnTypeOid);

    TPgResolvedCall(const std::string_view& name,
                    ui32 id,
                    const TVector<ui32>& argTypeIds,
                    ui32 returnTypeOid);

    THolder<TPgResolvedCallState> CreateState() const;

    using TValueGetter = std::function<NUdf::TUnboxedValue(size_t index)>;

    template <typename TValueGetter>
    NUdf::TUnboxedValue CallFunction(TPgResolvedCallState& state, TValueGetter valueGetter) const requires std::invocable<TValueGetter, size_t> {
        auto& callInfo = state.CallInfo.Ref();
        auto& args = state.Args;
        if constexpr (UseContext) {
            callInfo.context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;
        }

        callInfo.isnull = false;
        for (ui32 i = 0; i < ArgDesc.size(); ++i) {
            args[i] = std::invoke(valueGetter, i);
            auto& value = args[i];
            NullableDatum argDatum = {0, false};
            if (!value) {
                if (this->FInfo.fn_strict) {
                    return NUdf::TUnboxedValuePod();
                }

                argDatum.isnull = true;
            } else {
                argDatum.value = this->ArgDesc[i].PassByValue ? ScalarDatumFromPod(value) : PointerDatumFromPod(value);
            }

            callInfo.args[i] = argDatum;
        }

        const bool needToFree = PrepareVariadicArray(callInfo, this->ProcDesc);
        NUdf::TUnboxedValue res;
        if constexpr (!UseContext) {
            NKikimr::NMiniKQL::TPAllocScope call;
            Y_DEFER {
                // This ensures that there is no dangling pointers references to freed
                // |TPAllocScope| pages that can be allocated and stored inside |callInfo.flinfo->fn_extra|.
                callInfo.flinfo->fn_extra = nullptr;
            };
            res = this->DoCall(callInfo);
        } else {
            res = this->DoCall(callInfo);
        }

        if (needToFree) {
            FreeVariadicArray(callInfo, this->ArgDesc.size());
        }

        return res;
    };

    NUdf::TUnboxedValue CallFunction(TPgResolvedCallState& state, NKikimr::NMiniKQL::TUnboxedValueView values) const;

private:
    NUdf::TUnboxedValue DoCall(FunctionCallInfoBaseData& callInfo) const;
};

extern template class TPgResolvedCall<true>;
extern template class TPgResolvedCall<false>;

struct TPgCastState {
    TPgCastState(const FmgrInfo* finfo1, const FmgrInfo* finfo2)
        : CallInfo1(3, finfo1)
        , CallInfo2(1, finfo2)
    {
    }

    TFunctionCallInfo CallInfo1, CallInfo2;
};

class TPgCast {
public:
    TPgCast(ui32 sourceId, ui32 targetId, bool hasTypeMod);

    NUdf::TUnboxedValue Calculate(NUdf::TUnboxedValue value, i32 typeMod, TPgCastState& state) const;

    TPgCastState CreateState() const {
        return TPgCastState(&FInfo1, &FInfo2);
    }

    const FmgrInfo* GetFInfo1() const {
        return &FInfo1;
    }
    const FmgrInfo* GetFInfo2() const {
        return &FInfo2;
    }

protected:
    Datum ConvertDatum(Datum datum, TPgCastState& state, i32 typeMod) const;

    const ui32 SourceId;
    const ui32 TargetId;
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

struct TPgResolvedCallWithCastState {
    TPgResolvedCallWithCastState(const TPgResolvedCall<false>& caller, const TVector<TPgCast>& casters);

    TPgResolvedCallState CallState;
    TVector<THolder<TPgCastState>> CastStates;
};

class TPgResolvedCallWithCast {
public:
    static TPgResolvedCallWithCast ForProc(std::string_view name, const TVector<ui32>& inputArgTypeIds);

    static TPgResolvedCallWithCast ForOperator(std::string_view operName, const TVector<ui32>& inputArgTypeIds);

    NUdf::TUnboxedValue CallFunctionWithCast(
        TPgResolvedCallWithCastState& state,
        NKikimr::NMiniKQL::TUnboxedValueView values) const;

    TPgResolvedCallWithCastState CreateState() const;

    ui32 GetReturnTypeId() const {
        return Call->GetReturnTypeId();
    }

    const TPgResolvedCall<false>& GetCall() const {
        return *Call;
    }

    const TVector<TPgCast>& GetCasters() const {
        return Casters;
    }

private:
    TPgResolvedCallWithCast(std::string_view name, ui32 procId,
                            const TVector<ui32>& expectedArgTypeIds,
                            const TVector<ui32>& inputArgTypeIds, ui32 resultType);

    std::unique_ptr<TPgResolvedCall<false>> Call;
    TVector<TPgCast> Casters;
};

} // namespace NYql
