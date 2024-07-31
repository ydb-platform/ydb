#pragma once

#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.cpp>
#include <arrow/compute/kernel.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_block_agg_factory.h>

#include "arena_ctx.h"

#include <functional>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "varatt.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
}

#include "utils.h"

namespace NYql {

struct TPgKernelState : arrow::compute::KernelState {
    FmgrInfo    flinfo;         /* lookup info used for this call */
    fmNodePtr   context;        /* pass info about context of call */
    fmNodePtr   resultinfo;     /* pass or return extra info about result */
    Oid         fncollation;    /* collation for function to use */
    TString     Name;
    std::vector<bool> IsFixedArg;
    bool IsFixedResult;
    i32 TypeLen;
    std::shared_ptr<void> FmgrDataHolder;
    const NPg::TProcDesc* ProcDesc;
};

template <PGFunction PgFunc>
struct TPgDirectFunc {
    Datum operator()(FunctionCallInfo info) const {
        return PgFunc(info);
    }
};

struct TPgIndirectFunc {
    TPgIndirectFunc(PGFunction pgFunc)
        : PgFunc(pgFunc)
    {}

    Datum operator()(FunctionCallInfo info) const {
        return PgFunc(info);
    }

    PGFunction PgFunc;
};

template <bool IsFixed>
Datum CloneDatumToAggContext(Datum src, i32 typeLen) {
    if constexpr (IsFixed) {
        return src;
    } else {
        Y_ENSURE(NKikimr::NMiniKQL::TlsAllocState->CurrentContext);
        ui32 len;
        if (typeLen == -1) {
            len = GetFullVarSize((const text*)src);
        } else if (typeLen == -2) {
            len = 1 + strlen((const char*)src);
        } else {
            len = typeLen;
        }

        auto ret = (Datum)palloc(len);
        memcpy((void*)ret, (const void*)src, len);
        return ret;
    }
}

template <bool IsFixed>
void CopyState(NullableDatum src, NullableDatum& dst) {
    if constexpr (IsFixed) {
        dst = src;
    } else {
        if (src.isnull == dst.isnull && src.value == dst.value) {
            return;
        }

        if (!dst.isnull) {
            pfree((void*)dst.value);
        }

        dst = src;
    }
}

template <bool IsFixed>
void SaveToAggContext(NullableDatum& d, i32 typeLen) {
    if constexpr (IsFixed) {
        return;
    }

    if (d.isnull) {
        return;
    }

    // arrow Scalars/Arrays have null memory context
    if (NUdf::GetMemoryContext((void*)d.value)) {
        return;
    }

    d.value = CloneDatumToAggContext<false>(d.value, typeLen);
}

template <typename TArgsPolicy>
struct TInputArgsAccessor {
    std::array<NullableDatum, TArgsPolicy::IsFixedArg.size()> Scalars;
    std::array<bool, TArgsPolicy::IsFixedArg.size()> IsScalar;
    std::array<ui64, TArgsPolicy::IsFixedArg.size()> Offsets;
    std::array<const ui8*, TArgsPolicy::IsFixedArg.size()> ValidMasks;
    std::array<ui64, TArgsPolicy::IsFixedArg.size()> ValidOffsetMask;
    ui8 fakeValidByte = 0xFF;
    std::array<const ui64*, TArgsPolicy::IsFixedArg.size()> FixedArrays;
    std::array<const ui32*, TArgsPolicy::IsFixedArg.size()> StringOffsetsArrays;
    std::array<const ui8*, TArgsPolicy::IsFixedArg.size()> StringDataArrays;

    void Bind(const std::vector<arrow::Datum>& values, size_t skipArgs = 0, TMaybe<size_t> realArgsCount = {}) {
        if constexpr (!TArgsPolicy::VarArgs) {
            const size_t argCount = realArgsCount.GetOrElse(TArgsPolicy::IsFixedArg.size());
            Y_ENSURE(argCount == values.size() + skipArgs);
            for (size_t j = skipArgs; j < argCount; ++j) {
                IsScalar[j] = values[j - skipArgs].is_scalar();
                if (IsScalar[j]) {
                    const auto& scalar = *values[j - skipArgs].scalar();
                    if (!scalar.is_valid) {
                        Scalars[j].isnull = true;
                    } else {
                        Scalars[j].isnull = false;
                        if (TArgsPolicy::IsFixedArg[j]) {
                            Scalars[j].value = (Datum)*static_cast<const ui64*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data());
                        } else {
                            auto buffer = arrow::internal::checked_cast<const arrow::BaseBinaryScalar&>(scalar).value;
                            Scalars[j].value = (Datum)(buffer->data() + sizeof(void*));
                        }
                    }
                } else {
                    const auto& array = *values[j - skipArgs].array();
                    Offsets[j] = array.offset;
                    ValidMasks[j] = array.GetValues<ui8>(0, 0);
                    if (ValidMasks[j]) {
                        ValidOffsetMask[j] = ~0ull;
                    } else {
                        ValidOffsetMask[j] = 0ull;
                        ValidMasks[j] = &fakeValidByte;
                    }
                    if (TArgsPolicy::IsFixedArg[j]) {
                        FixedArrays[j] = array.GetValues<ui64>(1);
                    } else {
                        StringOffsetsArrays[j] = array.GetValues<ui32>(1);
                        StringDataArrays[j] = array.GetValues<ui8>(2, 0);
                    }
                }
            }
        }
    }
};

template <bool HasNulls, bool IsFixed>
void FillScalarItem(const arrow::Scalar& scalar, NullableDatum& d) {
    if constexpr (IsFixed) {
        NUdf::TFixedSizeBlockReader<ui64, HasNulls> reader;
        auto item = reader.GetScalarItem(scalar);
        if (HasNulls && !item) {
            d.isnull = true;
        } else {
            d.isnull = false;
            d.value = (Datum)item.template As<ui64>();
        }
    } else {
        NUdf::TStringBlockReader<arrow::BinaryType, HasNulls> reader;
        auto item = reader.GetScalarItem(scalar);
        if (HasNulls && !item) {
            d.isnull = true;
        } else {
            d.isnull = false;
            d.value = (Datum)(item.AsStringRef().Data() + sizeof(void*));
        }
    }
}

template <bool HasNulls, bool IsFixed>
void FillArrayItem(const arrow::ArrayData& array, size_t i, NullableDatum& d) {
    if constexpr (IsFixed) {
        NUdf::TFixedSizeBlockReader<ui64, HasNulls> reader;
        auto item = reader.GetItem(array, i);
        if (HasNulls && !item) {
            d.isnull = true;
        } else {
            d.isnull = false;
            d.value = (Datum)item.template As<ui64>();
        }
    } else {
        NUdf::TStringBlockReader<arrow::BinaryType, HasNulls> reader;
        auto item = reader.GetItem(array, i);
        if (HasNulls && !item) {
            d.isnull = true;
        } else {
            d.isnull = false;
            d.value = (Datum)(item.AsStringRef().Data() + sizeof(void*));
        }
    }
}

template <auto Start, auto End, auto Inc, class F>
constexpr bool constexpr_for(F&& f) {
    if constexpr (Start < End) {
        if (!f(std::integral_constant<decltype(Start), Start>())) {
            return false;
        }

        return constexpr_for<Start + Inc, End, Inc>(f);
    }

    return true;
}

template <class F, class Tuple>
constexpr bool constexpr_for_tuple(F&& f, Tuple&& tuple) {
    constexpr size_t cnt = std::tuple_size_v<std::decay_t<Tuple>>;

    return constexpr_for<size_t(0), cnt, size_t(1)>([&](auto i) {
        return f(i.value, std::get<i.value>(tuple));
    });
}

enum class EScalarArgBinary {
    Unknown,
    First,
    Second
};

struct TDefaultArgsPolicy {
    static constexpr bool VarArgs = true;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};

Y_PRAGMA_DIAGNOSTIC_PUSH
Y_PRAGMA("GCC diagnostic ignored \"-Wreturn-type-c-linkage\"")
extern "C" TPgKernelState& GetPGKernelState(arrow::compute::KernelContext* ctx);
Y_PRAGMA_DIAGNOSTIC_POP

template <typename TFunc, bool IsStrict, bool IsFixedResult, typename TArgsPolicy = TDefaultArgsPolicy>
struct TGenericExec {
    TGenericExec(TFunc func)
        : Func(func)
    {}

    Y_NO_INLINE arrow::Status operator()(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        auto& state = GetPGKernelState(ctx);
        if constexpr (!TArgsPolicy::VarArgs) {
            Y_ENSURE(batch.values.size() == TArgsPolicy::IsFixedArg.size());
            Y_ENSURE(state.IsFixedArg.size() == TArgsPolicy::IsFixedArg.size());
            for (ui32 i = 0; i < TArgsPolicy::IsFixedArg.size(); ++i) {
                Y_ENSURE(state.IsFixedArg[i] == TArgsPolicy::IsFixedArg[i]);
            }
        }

        size_t length = 1;
        bool hasNulls = false;
        bool hasArrays = false;
        bool hasScalars = false;
        for (const auto& v : batch.values) {
            if (v.is_array()) {
                length = v.array()->length;
                if (v.array()->GetNullCount() > 0) {
                    hasNulls = true;
                }

                hasArrays = true;
            } else {
                hasScalars = true;
                if (!v.scalar()->is_valid) {
                    hasNulls = true;
                }
            }
        }

        Y_ENSURE(hasArrays);
        Y_ENSURE(state.flinfo.fn_strict == IsStrict);
        Y_ENSURE(state.IsFixedResult == IsFixedResult);
        TArenaMemoryContext arena;
        Dispatch1(hasScalars, hasNulls, ctx, batch, length, state, res);
        return arrow::Status::OK();
    }

    Y_NO_INLINE void Dispatch1(bool hasScalars, bool hasNulls, arrow::compute::KernelContext* ctx,
        const arrow::compute::ExecBatch& batch, size_t length, TPgKernelState& state, arrow::Datum* res) const {
        if (hasScalars) {
            if (hasNulls) {
                if constexpr (IsFixedResult) {
                    NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<true, true>(batch, length, state, builder);
                } else {
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::binary(), *ctx->memory_pool(), length);
                    *res = Dispatch2<true, true>(batch, length, state, builder);
                }
            } else {
                if constexpr (IsFixedResult) {
                    NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<true, false>(batch, length, state, builder);
                } else {
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::binary(), *ctx->memory_pool(), length);
                    *res = Dispatch2<true, false>(batch, length, state, builder);
                }
            }
        } else {
            if (hasNulls) {
                if constexpr (IsFixedResult) {
                    NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<false, true>(batch, length, state, builder);
                } else {
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::binary(), *ctx->memory_pool(), length);
                    *res = Dispatch2<false, true>(batch, length, state, builder);
                }
            } else {
                if constexpr (IsFixedResult) {
                    NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<false, false>(batch, length, state, builder);
                } else {
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::binary(), *ctx->memory_pool(), length);
                    *res = Dispatch2<false, false>(batch, length, state, builder);
                }
            }
        }
    }

    template <bool HasScalars, bool HasNulls, typename TBuilder>
    Y_NO_INLINE arrow::Datum Dispatch2(const arrow::compute::ExecBatch& batch, size_t length, TPgKernelState& state, TBuilder& builder) const {
        if constexpr (!TArgsPolicy::VarArgs) {
            if (TArgsPolicy::IsFixedArg.size() == 2) {
                if (batch.values[0].is_scalar()) {
                    return Dispatch3<HasScalars, HasNulls, EScalarArgBinary::First>(batch, length, state, builder);                
                }

                if (batch.values[1].is_scalar()) {
                    return Dispatch3<HasScalars, HasNulls, EScalarArgBinary::Second>(batch, length, state, builder);                
                }
            }
        }

        return Dispatch3<HasScalars, HasNulls, EScalarArgBinary::Unknown>(batch, length, state, builder);
    }

    template <bool HasScalars, bool HasNulls, EScalarArgBinary ScalarArgBinary, typename TBuilder>
    Y_NO_INLINE arrow::Datum Dispatch3(const arrow::compute::ExecBatch& batch, size_t length, TPgKernelState& state, TBuilder& builder) const {
        LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
        fcinfo->flinfo = &state.flinfo;
        fcinfo->context = state.context;
        fcinfo->resultinfo = state.resultinfo;
        fcinfo->fncollation = state.fncollation;
        fcinfo->nargs = batch.values.size();

        TInputArgsAccessor<TArgsPolicy> inputArgsAccessor;
        inputArgsAccessor.Bind(batch.values);

        ui64* fixedResultData = nullptr;
        ui8* fixedResultValidMask = nullptr;
        if constexpr (IsFixedResult) {
            builder.UnsafeReserve(length);
            fixedResultData = builder.MutableData();
            fixedResultValidMask = builder.MutableValidMask();
        }

        for (size_t i = 0; i < length; ++i) {
            Datum ret;
            bool needToFree = false;
            if constexpr (!TArgsPolicy::VarArgs) {
                if (!constexpr_for_tuple([&](auto const& j, auto const& v) {
                    NullableDatum d;
                    if (HasScalars && (
                        (ScalarArgBinary == EScalarArgBinary::First && j == 0) || 
                        (ScalarArgBinary == EScalarArgBinary::Second && j == 1) || 
                        inputArgsAccessor.IsScalar[j])) {
                        d = inputArgsAccessor.Scalars[j];
                    } else {
                        d.isnull = false;
                        if constexpr (HasNulls) {
                            ui64 fullIndex = (i + inputArgsAccessor.Offsets[j]) & inputArgsAccessor.ValidOffsetMask[j];
                            d.isnull = ((inputArgsAccessor.ValidMasks[j][fullIndex >> 3] >> (fullIndex & 0x07)) & 1) == 0;
                        }

                        if (v) {
                            d.value = (Datum)inputArgsAccessor.FixedArrays[j][i];
                        } else {
                            d.value = (Datum)(sizeof(void*) + inputArgsAccessor.StringOffsetsArrays[j][i] + inputArgsAccessor.StringDataArrays[j]);
                        }
                    }

                    if (HasNulls && IsStrict && d.isnull) {
                        return false;
                    }

                    fcinfo->args[j] = d;
                    return true;            
                }, TArgsPolicy::IsFixedArg)) {
                    if constexpr (IsFixedResult) {
                        fixedResultValidMask[i] = 0;
                    } else {
                        builder.Add(NUdf::TBlockItem{});
                    }
                    goto SkipCall;
                }
            } else {
                for (size_t j = 0; j < batch.values.size(); ++j) {
                    NullableDatum d;
                    if (HasScalars && batch.values[j].is_scalar()) {
                        if (state.IsFixedArg[j]) {
                            FillScalarItem<HasNulls, true>(*batch.values[j].scalar(), d);
                        } else {
                            FillScalarItem<HasNulls, false>(*batch.values[j].scalar(), d);
                        }
                    } else {
                        if (state.IsFixedArg[j]) {
                            FillArrayItem<HasNulls, true>(*batch.values[j].array(), i, d);
                        } else {
                            FillArrayItem<HasNulls, false>(*batch.values[j].array(), i, d);
                        }
                    }

                    if (HasNulls && IsStrict && d.isnull) {
                        if constexpr (IsFixedResult) {
                            fixedResultValidMask[i] = 0;
                        } else {
                            builder.Add(NUdf::TBlockItem{});
                        }
                        goto SkipCall;
                    }

                    fcinfo->args[j] = d;
                }
            }

            fcinfo->isnull = false;
            if constexpr (TArgsPolicy::VarArgs) {
                needToFree = PrepareVariadicArray(*fcinfo, *state.ProcDesc);
            }

            ret = Func(fcinfo);
            if constexpr (TArgsPolicy::VarArgs) {
                if (needToFree) {
                    FreeVariadicArray(*fcinfo, batch.values.size());
                }
            }

            if constexpr (IsFixedResult) {
                fixedResultData[i] = ui64(ret);
                fixedResultValidMask[i] = !fcinfo->isnull;
            } else {
                if (fcinfo->isnull) {
                    builder.Add(NUdf::TBlockItem{});
                } else {
                    auto ptr = (char*)ret;
                    ui32 len;
                    if (state.TypeLen == -1) {
                        len = GetFullVarSize((const text*)ptr);
                    } else if (state.TypeLen == -2) {
                        len = 1 + strlen(ptr);
                    } else {
                        len = state.TypeLen;
                    }

                    builder.AddPgItem(NUdf::TStringRef(ptr, len));
                }
            }
    SkipCall:;
        }

        return builder.Build(true);
    }

    TFunc Func;
};

using TExecFunc = std::function<arrow::Status(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res)>;

template <bool IsStrict, bool IsFixedResult>
TExecFunc MakeIndirectExec(PGFunction pgFunc) {
    return TGenericExec<TPgIndirectFunc, IsStrict, IsFixedResult>(TPgIndirectFunc(pgFunc));
}

template <bool IsFixed, typename TArgsPolicy>
NullableDatum GetInputValue(const TInputArgsAccessor<TArgsPolicy>& accessor, ui32 j, ui64 row) {
    static_assert(!TArgsPolicy::VarArgs);
    if (accessor.IsScalar[j]) {
        return accessor.Scalars[j];
    } else {
        NullableDatum d;
        ui64 fullIndex = (row + accessor.Offsets[j]) & accessor.ValidOffsetMask[j];
        d.isnull = ((accessor.ValidMasks[j][fullIndex >> 3] >> (fullIndex & 0x07)) & 1) == 0;

        if constexpr (IsFixed) {
            d.value = (Datum)accessor.FixedArrays[j][row];
        } else {
            d.value = (Datum)(sizeof(void*) + accessor.StringOffsetsArrays[j][row] + accessor.StringDataArrays[j]);
        }

        return d;
    }
}

template <bool IsFixed>
NullableDatum GetInputValueSlow(const std::vector<arrow::Datum>& values, ui32 j, ui64 row) {
    NullableDatum d;
    if (values[j].is_scalar()) {
        if constexpr (IsFixed) {
            FillScalarItem<true, true>(*values[j].scalar(), d);
        } else {
            FillScalarItem<true, false>(*values[j].scalar(), d);
        }
    } else {
        if constexpr (IsFixed) {
            FillArrayItem<true, true>(*values[j].array(), row, d);
        } else {
            FillArrayItem<true, false>(*values[j].array(), row, d);
        }
    }

    return d;
}

template <bool IsFixed, bool HasFunc, typename TFunc, bool IsStrict, typename TBuilder>
class TAggColumnBuilder : public NKikimr::NMiniKQL::IAggColumnBuilder {
public:
    TAggColumnBuilder(const TString& name, TFunc func, ui64 size, FmgrInfo* funcInfo, const std::shared_ptr<arrow::DataType>& dataType,
        NKikimr::NMiniKQL::TComputationContext& ctx, i32 typeLen)
        : Name_(name)
        , Func_(func)
        , FuncInfo_(funcInfo)
        , Builder_(NKikimr::NMiniKQL::TTypeInfoHelper(), dataType, ctx.ArrowMemoryPool, size)
        , Ctx_(ctx)
        , TypeLen_(typeLen)
    {
    }

    void Add(const void* state) final {
        auto typedState = (NullableDatum*)state;
        auto ret = *typedState;
        if constexpr (HasFunc) {
            if (!IsStrict || !typedState->isnull) {
                LOCAL_FCINFO(callInfo, 1);
                callInfo->flinfo = FuncInfo_;
                callInfo->nargs = 1;
                callInfo->fncollation = DEFAULT_COLLATION_OID;
                callInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;
                callInfo->isnull = false;
                callInfo->args[0].isnull = typedState->isnull;
                callInfo->args[0].value = typedState->value;
                ret.value = Func_(callInfo);
                ret.isnull = callInfo->isnull;
            }
        }

        if (ret.isnull) {
            Builder_.Add(NYql::NUdf::TBlockItem());
        } else {
            if constexpr (IsFixed) {
                Builder_.Add(NYql::NUdf::TBlockItem(ui64(ret.value)));
            } else if (TypeLen_ == -1) {
                auto ptr = (char*)ret.value;
                ui32 len = GetFullVarSize((const text*)ptr);
                Builder_.AddPgItem(NYql::NUdf::TStringRef(ptr, len));
            } else if (TypeLen_ == -2) {
                auto ptr = (char*)ret.value;
                ui32 len = 1 + strlen(ptr);
                Builder_.AddPgItem(NYql::NUdf::TStringRef(ptr, len));
            } else {
                auto ptr = (char*)ret.value;
                ui32 len = TypeLen_;
                Builder_.AddPgItem(NYql::NUdf::TStringRef(ptr, len));
            }
        }
    }

    NUdf::TUnboxedValue Build() final {
        return Ctx_.HolderFactory.CreateArrowBlock(Builder_.Build(true));
    }

private:
    const TString Name_;
    const TFunc Func_;
    FmgrInfo* FuncInfo_;
    TBuilder Builder_;
    NKikimr::NMiniKQL::TComputationContext& Ctx_;
    const i32 TypeLen_;
};

template <typename TTransFunc, bool IsTransStrict, typename TTransArgsPolicy,
    typename TCombineFunc, bool IsCombineStrict, typename TCombineArgsPolicy,
    bool HasSerialize, typename TSerializeFunc, typename TSerializeArgsPolicy,
    bool HasDeserialize, typename TDeserializeFunc, typename TDeserializeArgsPolicy,
    bool HasFinal, typename TFinalFunc, bool IsFinalStrict, typename TFinalArgsPolicy,
    bool IsTransTypeFixed, bool IsSerializedTypeFixed, bool IsFinalTypeFixed, bool HasInitValue>
class TGenericAgg {
public:
    TGenericAgg(TTransFunc transFunc, TCombineFunc combineFunc, TSerializeFunc serializeFunc,
        TDeserializeFunc deserializeFunc, TFinalFunc finalFunc)
        : TransFunc(transFunc)
        , CombineFunc(combineFunc)
        , SerializeFunc(serializeFunc)
        , DeserializeFunc(deserializeFunc)
        , FinalFunc(finalFunc)
    {}

private:
    template <typename TAggregatorBase>
    class TCombineAggregatorBase: public TAggregatorBase {
    protected:
        TCombineAggregatorBase(TTransFunc transFunc, TSerializeFunc serializeFunc, const std::vector<ui32>& argsColumns,
            std::optional<ui32> filterColumn, const NPg::TAggregateDesc& aggDesc, NKikimr::NMiniKQL::TComputationContext& ctx)
            : TAggregatorBase(sizeof(NullableDatum), filterColumn, ctx)
            , TransFunc_(transFunc)
            , SerializeFunc_(serializeFunc)
            , ArgsColumns_(argsColumns)
            , AggDesc_(aggDesc)
            , TransTypeLen_(NPg::LookupType(this->AggDesc_.TransTypeId).TypeLen)
        {
            if (!HasInitValue && IsTransStrict) {
                Y_ENSURE(AggDesc_.ArgTypes.size() == 1);
            }
            
            const auto& transDesc = NPg::LookupProc(AggDesc_.TransFuncId);
            for (ui32 i = 1; i < transDesc.ArgTypes.size(); ++i) {
                IsFixedArg_.push_back(NPg::LookupType(transDesc.ArgTypes[i]).PassByValue);
            }

            Zero(TransFuncInfo_);
            GetPgFuncAddr(AggDesc_.TransFuncId, TransFuncInfo_);
            Y_ENSURE(TransFuncInfo_.fn_addr);
            auto nargs = NPg::LookupProc(AggDesc_.TransFuncId).ArgTypes.size();
            if constexpr (HasSerialize) {
                Zero(SerializeFuncInfo_);
                GetPgFuncAddr(AggDesc_.SerializeFuncId, SerializeFuncInfo_);
                Y_ENSURE(SerializeFuncInfo_.fn_addr);
            }

            if constexpr (HasInitValue) {
                Zero(InFuncInfo_);
                const auto& transTypeDesc = NPg::LookupType(AggDesc_.TransTypeId);
                auto inFuncId = transTypeDesc.InFuncId;
                if (transTypeDesc.TypeId == transTypeDesc.ArrayTypeId) {
                    inFuncId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
                }

                TypeIOParam_ = MakeTypeIOParam(transTypeDesc);
                GetPgFuncAddr(inFuncId, InFuncInfo_);
                Y_ENSURE(InFuncInfo_.fn_addr);

                LOCAL_FCINFO(inCallInfo, 3);
                inCallInfo->flinfo = &this->InFuncInfo_;
                inCallInfo->nargs = 3;
                inCallInfo->fncollation = DEFAULT_COLLATION_OID;
                inCallInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;
                inCallInfo->isnull = false;
                inCallInfo->args[0] = { (Datum)this->AggDesc_.InitValue.c_str(), false };
                inCallInfo->args[1] = { ObjectIdGetDatum(this->TypeIOParam_), false };
                inCallInfo->args[2] = { Int32GetDatum(-1), false };

                auto state = this->InFuncInfo_.fn_addr(inCallInfo);
                Y_ENSURE(!inCallInfo->isnull);
                PreparedInitValue_ = AnyDatumToPod(state, IsTransTypeFixed);
            }
        }

        const TTransFunc TransFunc_;
        const TSerializeFunc SerializeFunc_;
        const std::vector<ui32> ArgsColumns_;
        const NPg::TAggregateDesc& AggDesc_;
        const i32 TransTypeLen_;
        std::vector<bool> IsFixedArg_;
        FmgrInfo TransFuncInfo_;
        FmgrInfo SerializeFuncInfo_;
        FmgrInfo InFuncInfo_;
        ui32 TypeIOParam_ = 0;
        NKikimr::NUdf::TUnboxedValue PreparedInitValue_;
    };

    template <bool HasFilter>
    class TCombineAllAggregator : public TCombineAggregatorBase<NKikimr::NMiniKQL::TCombineAllTag::TBase> {
    public:
        using TBase = TCombineAggregatorBase<NKikimr::NMiniKQL::TCombineAllTag::TBase>;
        TCombineAllAggregator(TTransFunc transFunc, TSerializeFunc serializeFunc, const std::vector<ui32>& argsColumns,
            std::optional<ui32> filterColumn, const NPg::TAggregateDesc& aggDesc, NKikimr::NMiniKQL::TComputationContext& ctx)
            : TBase(transFunc, serializeFunc, argsColumns, filterColumn, aggDesc, ctx)
        {
            Y_ENSURE(HasFilter == filterColumn.has_value());
        }

    private:
        void DestroyState(void* state) noexcept final {
            Y_UNUSED(state);
        }

        void InitState(void* state) final {
            new(state) NullableDatum();
            auto typedState = (NullableDatum*)state;
            typedState->isnull = true;
            typedState->value = 0;
            if constexpr (HasInitValue) {
                auto datum = IsTransTypeFixed ? ScalarDatumFromPod(this->PreparedInitValue_) : PointerDatumFromPod(this->PreparedInitValue_);
                typedState->isnull = false;
                typedState->value = CloneDatumToAggContext<IsTransTypeFixed>(datum, this->TransTypeLen_);
            }
        }

        void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
            auto typedState = (NullableDatum*)state;
            std::vector<arrow::Datum> values;
            values.reserve(this->ArgsColumns_.size());
            for (auto col : this->ArgsColumns_) {
                values.push_back(NKikimr::NMiniKQL::TArrowBlock::From(columns[col]).GetDatum());
            }

            bool hasNulls = false;
            bool hasScalars = false;
            for (const auto& v : values) {
                if (v.is_array()) {
                    if (v.array()->GetNullCount() > 0) {
                        hasNulls = true;
                    }
                } else {
                    hasScalars = true;
                    if (!v.scalar()->is_valid) {
                        hasNulls = true;
                    }
                }
            }

            const ui8* filterBitmap = nullptr;
            if constexpr(HasFilter) {
                const auto& filterDatum = NKikimr::NMiniKQL::TArrowBlock::From(columns[*this->FilterColumn_]).GetDatum();
                const auto& filterArray = filterDatum.array();
                Y_ENSURE(filterArray->GetNullCount() == 0);
                filterBitmap = filterArray->template GetValues<uint8_t>(1);
            }

            if (hasNulls) {
                if (hasScalars) {
                    AddManyImpl<true, true>(typedState, values, batchLength, filterBitmap);
                } else {
                    AddManyImpl<true, false>(typedState, values, batchLength, filterBitmap);
                }
            } else {
                if (hasScalars) {
                    AddManyImpl<false, true>(typedState, values, batchLength, filterBitmap);
                } else {
                    AddManyImpl<false, false>(typedState, values, batchLength, filterBitmap);
                }
            }
        }

        template <bool HasNulls, bool HasScalars>
        void AddManyImpl(NullableDatum* typedState, const std::vector<arrow::Datum>& values, ui64 batchLength, const ui8* filterBitmap) {
            LOCAL_FCINFO(transCallInfo, FUNC_MAX_ARGS);
            transCallInfo->flinfo = &this->TransFuncInfo_;
            if constexpr (!TTransArgsPolicy::VarArgs) {
                transCallInfo->nargs = TTransArgsPolicy::IsFixedArg.size();
            } else {
                transCallInfo->nargs = 1 + values.size();
            }

            transCallInfo->fncollation = DEFAULT_COLLATION_OID;
            transCallInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;

            transCallInfo->args[0] = *typedState;

            TInputArgsAccessor<TTransArgsPolicy> inputArgsAccessor;
            inputArgsAccessor.Bind(values, 1);

            for (ui64 i = 0; i < batchLength; ++i) {
                if constexpr (HasFilter) {
                    if (!filterBitmap[i]) {
                        continue;
                    }
                }

                Datum ret;
                if constexpr (!TTransArgsPolicy::VarArgs) {
                    if (!constexpr_for_tuple([&](auto const& j, auto const& v) {
                        if (j == 0) {
                            return true;
                        }

                        NullableDatum d;
                        if (HasScalars && inputArgsAccessor.IsScalar[j]) {
                            d = inputArgsAccessor.Scalars[j];
                        } else {
                            d.isnull = false;
                            if constexpr (HasNulls) {
                                ui64 fullIndex = (i + inputArgsAccessor.Offsets[j]) & inputArgsAccessor.ValidOffsetMask[j];
                                d.isnull = ((inputArgsAccessor.ValidMasks[j][fullIndex >> 3] >> (fullIndex & 0x07)) & 1) == 0;
                            }

                            if (v) {
                                d.value = (Datum)inputArgsAccessor.FixedArrays[j][i];
                            } else {
                                d.value = (Datum)(sizeof(void*) + inputArgsAccessor.StringOffsetsArrays[j][i] + inputArgsAccessor.StringDataArrays[j]);
                            }
                        }

                        if (HasNulls && IsTransStrict && d.isnull) {
                            return false;
                        }

                        transCallInfo->args[j] = d;
                        return true;
                    }, TTransArgsPolicy::IsFixedArg)) {
                        goto SkipCall;
                    }
                } else {
                    for (size_t j = 0; j < values.size(); ++j) {
                        NullableDatum d;
                        if (HasScalars && values[j].is_scalar()) {
                            if (this->IsFixedArg_[j]) {
                                FillScalarItem<HasNulls, true>(*values[j].scalar(), d);
                            } else {
                                FillScalarItem<HasNulls, false>(*values[j].scalar(), d);
                            }
                        } else {
                            if (this->IsFixedArg_[j]) {
                                FillArrayItem<HasNulls, true>(*values[j].array(), i, d);
                            } else {
                                FillArrayItem<HasNulls, false>(*values[j].array(), i, d);
                            }
                        }

                        if (HasNulls && IsTransStrict && d.isnull) {
                            goto SkipCall;
                        }

                        transCallInfo->args[1 + j] = d;
                    }
                }

                if (!HasInitValue && IsTransStrict) {
                    if (transCallInfo->args[0].isnull) {
                        transCallInfo->args[0] = transCallInfo->args[1];
                        continue;
                    }
                }

                transCallInfo->isnull = false;
                ret = this->TransFunc_(transCallInfo);
                transCallInfo->args[0].value = ret;
                transCallInfo->args[0].isnull = transCallInfo->isnull;
SkipCall:;
            }

            CopyState<IsTransTypeFixed>(transCallInfo->args[0], *typedState);
            SaveToAggContext<IsTransTypeFixed>(*typedState, this->TransTypeLen_);
        }

        NUdf::TUnboxedValue FinishOne(const void* state) final {
            auto typedState = (NullableDatum*)state;
            if (typedState->isnull) {
                return {};
            }

            if constexpr (HasSerialize) {
                NUdf::TUnboxedValue ret;
                LOCAL_FCINFO(serializeCallInfo, 1);
                serializeCallInfo->flinfo = &this->SerializeFuncInfo_;
                serializeCallInfo->nargs = 1;
                serializeCallInfo->fncollation = DEFAULT_COLLATION_OID;
                serializeCallInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;
                serializeCallInfo->isnull = false;
                serializeCallInfo->args[0].isnull = false;
                serializeCallInfo->args[0].value = typedState->value;
                auto ser = this->SerializeFunc_(serializeCallInfo);
                Y_ENSURE(!serializeCallInfo->isnull);
                if constexpr (IsSerializedTypeFixed) {
                    ret = ScalarDatumToPod(ser);
                } else {
                    ret = PointerDatumToPod(ser);
                    if (ser == typedState->value) {
                        typedState->value = 0;
                        typedState->isnull = true;
                    }
                }

                return ret;
            } else {
                if constexpr (IsTransTypeFixed) {
                    return ScalarDatumToPod(typedState->value);
                } else {
                    auto ret = PointerDatumToPod(typedState->value);
                    typedState->value = 0;
                    typedState->isnull = true;
                    return ret;
                }
            }
        }
    };

    class TCombineKeysAggregator : public TCombineAggregatorBase<NKikimr::NMiniKQL::TCombineKeysTag::TBase> {
    public:
        using TBase = TCombineAggregatorBase<NKikimr::NMiniKQL::TCombineKeysTag::TBase>;
        TCombineKeysAggregator(TTransFunc transFunc, TSerializeFunc serializeFunc, const std::vector<ui32>& argsColumns,
            const NPg::TAggregateDesc& aggDesc, NKikimr::NMiniKQL::TComputationContext& ctx)
            : TBase(transFunc, serializeFunc, argsColumns, std::optional<ui32>(), aggDesc, ctx)
            , SerializedType_(HasSerialize ? NPg::LookupProc(this->AggDesc_.SerializeFuncId).ResultType : this->AggDesc_.TransTypeId)
        {
            Values_.reserve(this->IsFixedArg_.size());
        }

        void DestroyState(void* state) noexcept final {
            Y_UNUSED(state);
        }

        void PrepareBatch(ui64 batchNum, const NKikimr::NUdf::TUnboxedValue* columns) {
            Values_.clear();
            for (auto col : this->ArgsColumns_) {
                Values_.push_back(NKikimr::NMiniKQL::TArrowBlock::From(columns[col]).GetDatum());
            }

            InputArgsAccessor_.Bind(Values_, 1);
            BatchNum_ = batchNum;
        }
        
        void InitKey(void* state, ui64 batchNum, const NKikimr::NUdf::TUnboxedValue* columns, ui64 row) final {
            new(state) NullableDatum();
            auto typedState = (NullableDatum*)state;
            typedState->isnull = true;
            typedState->value = 0;
            if constexpr (HasInitValue) {
                auto datum = IsTransTypeFixed ? ScalarDatumFromPod(this->PreparedInitValue_) : PointerDatumFromPod(this->PreparedInitValue_);
                typedState->isnull = false;
                typedState->value = CloneDatumToAggContext<IsTransTypeFixed>(datum, this->TransTypeLen_);
            }

            UpdateKey(state, batchNum, columns, row);
        }

        void UpdateKey(void* state, ui64 batchNum, const NKikimr::NUdf::TUnboxedValue* columns, ui64 row) final {
            auto typedState = (NullableDatum*)state;
            if (batchNum != BatchNum_) {
                PrepareBatch(batchNum, columns);
            }

            LOCAL_FCINFO(transCallInfo, FUNC_MAX_ARGS);
            transCallInfo->flinfo = &this->TransFuncInfo_;
            if constexpr (!TTransArgsPolicy::VarArgs) {
                transCallInfo->nargs = TTransArgsPolicy::IsFixedArg.size();
            } else {
                transCallInfo->nargs = 1 + Values_.size();
            }

            transCallInfo->fncollation = DEFAULT_COLLATION_OID;
            transCallInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;

            transCallInfo->args[0] = *typedState;

            Datum ret;
            if constexpr (!TTransArgsPolicy::VarArgs) {
                if (!constexpr_for_tuple([&](auto const& j, auto const& v) {
                    if (j == 0) {
                        return true;
                    }

                    NullableDatum d;
                    if (v) {
                        d = GetInputValue<true>(InputArgsAccessor_, j, row);
                    } else {
                        d = GetInputValue<false>(InputArgsAccessor_, j, row);
                    }

                    if (IsTransStrict && d.isnull) {
                        return false;
                    }

                    transCallInfo->args[j] = d;
                    return true;
                }, TTransArgsPolicy::IsFixedArg)) {
                    return;
                }
            } else {
                for (size_t j = 0; j < Values_.size(); ++j) {
                    NullableDatum d;
                    if (this->IsFixedArg_[j]) {
                        d = GetInputValueSlow<true>(Values_, j, row);
                    } else {
                        d = GetInputValueSlow<false>(Values_, j, row);
                    }

                    if (IsTransStrict && d.isnull) {
                        return;
                    }

                    transCallInfo->args[1 + j] = d;
                }
            }

            if (!HasInitValue && IsTransStrict) {
                if (transCallInfo->args[0].isnull) {
                    typedState->isnull = false;
                    typedState->value = CloneDatumToAggContext<IsTransTypeFixed>(transCallInfo->args[1].value, this->TransTypeLen_);
                    return;
                }
            }

            transCallInfo->isnull = false;
            ret = this->TransFunc_(transCallInfo);

            CopyState<IsTransTypeFixed>({ret, transCallInfo->isnull}, *typedState);
            SaveToAggContext<IsTransTypeFixed>(*typedState, this->TransTypeLen_);
        }

        std::unique_ptr<NKikimr::NMiniKQL::IAggColumnBuilder> MakeStateBuilder(ui64 size) final {
            auto typeLen = NPg::LookupType(SerializedType_).TypeLen;
            if constexpr (IsSerializedTypeFixed) {
                return std::make_unique<TAggColumnBuilder<true, HasSerialize, TSerializeFunc, true, NYql::NUdf::TFixedSizeArrayBuilder<ui64, true>>>(
                    this->AggDesc_.Name, this->SerializeFunc_, size, &this->SerializeFuncInfo_, arrow::uint64(), this->Ctx_, typeLen);
            } else {
                return std::make_unique<TAggColumnBuilder<false, HasSerialize, TSerializeFunc, true, NYql::NUdf::TStringArrayBuilder<arrow::BinaryType, true, NYql::NUdf::EPgStringType::Text>>>(
                    this->AggDesc_.Name, this->SerializeFunc_, size, &this->SerializeFuncInfo_, arrow::binary(), this->Ctx_, typeLen);
            }
        }

        const ui32 SerializedType_;
        ui64 BatchNum_ = Max<ui64>();
        std::vector<arrow::Datum> Values_;
        TInputArgsAccessor<TTransArgsPolicy> InputArgsAccessor_;
    };

    class TFinalizeKeysAggregator : public NKikimr::NMiniKQL::TFinalizeKeysTag::TBase {
    public:
        using TBase = NKikimr::NMiniKQL::TFinalizeKeysTag::TBase;
        TFinalizeKeysAggregator(TDeserializeFunc deserializeFunc, TCombineFunc combineFunc, TFinalFunc finalFunc,
            ui32 stateColumn, const NPg::TAggregateDesc& aggDesc, NKikimr::NMiniKQL::TComputationContext& ctx)
            : TBase(sizeof(NullableDatum), std::optional<ui32>(), ctx)
            , DeserializeFunc_(deserializeFunc)
            , CombineFunc_(combineFunc)
            , FinalFunc_(finalFunc)
            , StateColumn_(stateColumn)
            , AggDesc_(aggDesc)
            , SerializedType_(HasSerialize ? NPg::LookupProc(this->AggDesc_.SerializeFuncId).ResultType : this->AggDesc_.TransTypeId)
            , FinalType_(HasFinal ? NPg::LookupProc(this->AggDesc_.FinalFuncId).ResultType : this->AggDesc_.TransTypeId)
            , TransTypeLen_(NPg::LookupType(this->AggDesc_.TransTypeId).TypeLen)
        {
            Values_.reserve(1);
        }

        void DestroyState(void* state) noexcept final {
            Y_UNUSED(state);
        }

        void PrepareBatch(ui64 batchNum, const NUdf::TUnboxedValue* columns) {
            Values_.clear();
            Values_.push_back(NKikimr::NMiniKQL::TArrowBlock::From(columns[StateColumn_]).GetDatum());
            if constexpr (HasDeserialize) {
                DeserializeAccessor_.Bind(Values_, 0, 1);
            } else {
                CombineAccessor_.Bind(Values_, 1);
            }

            BatchNum_ = batchNum;
        }

        void Deserialize(Datum ser, NullableDatum& result) {
            LOCAL_FCINFO(deserializeCallInfo, 1);
            deserializeCallInfo->flinfo = &this->DeserializeFuncInfo_;
            deserializeCallInfo->nargs = 1;
            deserializeCallInfo->fncollation = DEFAULT_COLLATION_OID;
            deserializeCallInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;
            deserializeCallInfo->isnull = false;
            deserializeCallInfo->args[0].isnull = false;
            deserializeCallInfo->args[0].value = ser;
            result.value = this->DeserializeFunc_(deserializeCallInfo);
            result.isnull = deserializeCallInfo->isnull;
        }

        void LoadState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
            new(state) NullableDatum();
            auto typedState = (NullableDatum*)state;
            typedState->isnull = true;
            typedState->value = 0;
            if (BatchNum_ != batchNum) {
                PrepareBatch(batchNum, columns);
            }

            NullableDatum d;
            if constexpr (HasDeserialize && !TDeserializeArgsPolicy::VarArgs) {
                d = GetInputValue<IsSerializedTypeFixed>(DeserializeAccessor_, 0, row);
            } else if constexpr (!HasDeserialize && !TCombineArgsPolicy::VarArgs) {
                d = GetInputValue<IsSerializedTypeFixed>(CombineAccessor_, 1, row);
            } else {
                d = GetInputValueSlow<IsSerializedTypeFixed>(Values_, 0, row);
            }

            if (d.isnull) {
                return;
            }

            if constexpr (!HasDeserialize) {
                typedState->isnull = false;
                typedState->value = CloneDatumToAggContext<IsTransTypeFixed>(d.value, this->TransTypeLen_);
            } else {
                Deserialize(d.value, *typedState);
            }

            SaveToAggContext<IsTransTypeFixed>(*typedState, this->TransTypeLen_);
        }

        void UpdateState(void* state, ui64 batchNum, const NUdf::TUnboxedValue* columns, ui64 row) final {
            auto typedState = (NullableDatum*)state;
            if (BatchNum_ != batchNum) {
                PrepareBatch(batchNum, columns);
            }

            NullableDatum d;
            if constexpr (HasDeserialize && !TDeserializeArgsPolicy::VarArgs) {
                d = GetInputValue<IsSerializedTypeFixed>(DeserializeAccessor_, 0, row);
            } else if constexpr (!HasDeserialize && !TCombineArgsPolicy::VarArgs) {
                d = GetInputValue<IsSerializedTypeFixed>(CombineAccessor_, 1, row);
            } else {
                d = GetInputValueSlow<IsSerializedTypeFixed>(Values_, 0, row);
            }

            if (IsCombineStrict && d.isnull) {
                return;
            }

            NullableDatum deser;
            if (d.isnull) {
                deser.isnull = true;
                deser.value = 0;
            } else {
                if constexpr (!HasDeserialize) {
                    if (IsCombineStrict && typedState->isnull) {
                        typedState->isnull = false;
                        typedState->value = CloneDatumToAggContext<IsTransTypeFixed>(d.value, this->TransTypeLen_);
                        return;
                    }

                    deser = d;
                } else {
                    Deserialize(d.value, deser);
                    if (IsCombineStrict && typedState->isnull) {
                        *typedState = deser;
                        return;
                    }
                }
            }

            LOCAL_FCINFO(combineCallInfo, 2);
            combineCallInfo->flinfo = &this->CombineFuncInfo_;
            combineCallInfo->nargs = 2;
            combineCallInfo->fncollation = DEFAULT_COLLATION_OID;
            combineCallInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;
            combineCallInfo->isnull = false;
            combineCallInfo->args[0] = *typedState;
            combineCallInfo->args[1] = deser;
            auto ret = this->CombineFunc_(combineCallInfo);
            if constexpr (!HasDeserialize) {                
                if (!combineCallInfo->isnull && ret == d.value) {
                    typedState->isnull = false;
                    typedState->value = CloneDatumToAggContext<IsTransTypeFixed>(d.value, this->TransTypeLen_);
                    return;
                }
            }

            CopyState<IsTransTypeFixed>({ret, combineCallInfo->isnull}, *typedState);
            SaveToAggContext<IsTransTypeFixed>(*typedState, this->TransTypeLen_);
        }

        std::unique_ptr<NKikimr::NMiniKQL::IAggColumnBuilder> MakeResultBuilder(ui64 size) final {
            auto typeLen = NPg::LookupType(FinalType_).TypeLen;
            if constexpr (IsFinalTypeFixed) {
                return std::make_unique<TAggColumnBuilder<true, HasFinal, TFinalFunc, IsFinalStrict, NYql::NUdf::TFixedSizeArrayBuilder<ui64, true>>>(
                    this->AggDesc_.Name, this->FinalFunc_, size, &this->FinalFuncInfo_, arrow::uint64(), this->Ctx_, typeLen);
            } else {
                return std::make_unique<TAggColumnBuilder<false, HasFinal, TFinalFunc, IsFinalStrict, NYql::NUdf::TStringArrayBuilder<arrow::BinaryType, true>>>(
                    this->AggDesc_.Name, this->FinalFunc_, size, &this->FinalFuncInfo_, arrow::binary(), this->Ctx_, typeLen);
            }
        }

        const TDeserializeFunc DeserializeFunc_;
        const TCombineFunc CombineFunc_;
        const TFinalFunc FinalFunc_;
        const ui32 StateColumn_;
        const NPg::TAggregateDesc& AggDesc_;
        const ui32 SerializedType_;
        const ui32 FinalType_;
        const i32 TransTypeLen_;
        ui64 BatchNum_ = Max<ui64>();
        std::vector<arrow::Datum> Values_;
        TInputArgsAccessor<TDeserializeArgsPolicy> DeserializeAccessor_;
        TInputArgsAccessor<TCombineArgsPolicy> CombineAccessor_;
        FmgrInfo DeserializeFuncInfo_;
        FmgrInfo CombineFuncInfo_;
        FmgrInfo FinalFuncInfo_;
    };

    class TPreparedCombineAllAggregator : public NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorCombineAll>{
    public:
        TPreparedCombineAllAggregator(TTransFunc transFunc, TSerializeFunc serializeFunc, const std::vector<ui32>& argsColumns,
            std::optional<ui32> filterColumn, const NPg::TAggregateDesc& aggDesc)
            : IPreparedBlockAggregator(sizeof(NullableDatum))
            , TransFunc(transFunc)
            , SerializeFunc(serializeFunc)
            , ArgsColumns(argsColumns)
            , FilterColumn(filterColumn)
            , AggDesc(aggDesc)
        {}
    private:

        std::unique_ptr<NKikimr::NMiniKQL::IBlockAggregatorCombineAll> Make(NKikimr::NMiniKQL::TComputationContext& ctx) const {
            if (FilterColumn.has_value()) {
                return std::make_unique<TCombineAllAggregator<true>>(TransFunc, SerializeFunc, ArgsColumns, FilterColumn, AggDesc, ctx);
            } else {
                return std::make_unique<TCombineAllAggregator<false>>(TransFunc, SerializeFunc, ArgsColumns, FilterColumn, AggDesc, ctx);
            }
        }

        const TTransFunc TransFunc;
        const TSerializeFunc SerializeFunc;
        const std::vector<ui32> ArgsColumns;
        const std::optional<ui32> FilterColumn;
        const NPg::TAggregateDesc& AggDesc;
    };

    class TPreparedCombineKeysAggregator : public NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorCombineKeys>{
    public:
        TPreparedCombineKeysAggregator(TTransFunc transFunc, TSerializeFunc serializeFunc, const std::vector<ui32>& argsColumns,
            const NPg::TAggregateDesc& aggDesc)
            : IPreparedBlockAggregator(sizeof(NullableDatum))
            , TransFunc(transFunc)
            , SerializeFunc(serializeFunc)
            , ArgsColumns(argsColumns)
            , AggDesc(aggDesc)
        {}

    private:
        std::unique_ptr<NKikimr::NMiniKQL::IBlockAggregatorCombineKeys> Make(NKikimr::NMiniKQL::TComputationContext& ctx) const {
            return std::make_unique<TCombineKeysAggregator>(TransFunc, SerializeFunc, ArgsColumns, AggDesc, ctx);
        }

        const TTransFunc TransFunc;
        const TSerializeFunc SerializeFunc;
        const std::vector<ui32> ArgsColumns;
        const NPg::TAggregateDesc& AggDesc;
    };

    class TPreparedFinalizeKeysAggregator : public NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorFinalizeKeys>{
    public:
        TPreparedFinalizeKeysAggregator(TDeserializeFunc deserializeFunc, TCombineFunc combineFunc, TFinalFunc finalFunc, ui32 stateColumn,
            const NPg::TAggregateDesc& aggDesc)
            : IPreparedBlockAggregator(sizeof(NullableDatum))
            , DeserializeFunc(deserializeFunc)
            , CombineFunc(combineFunc)
            , FinalFunc(finalFunc)
            , StateColumn(stateColumn)
            , AggDesc(aggDesc)
        {}

    private:
        std::unique_ptr<NKikimr::NMiniKQL::IBlockAggregatorFinalizeKeys> Make(NKikimr::NMiniKQL::TComputationContext& ctx) const {
            return std::make_unique<TFinalizeKeysAggregator>(DeserializeFunc, CombineFunc, FinalFunc, StateColumn, AggDesc, ctx);
        }

        const TDeserializeFunc DeserializeFunc;
        const TCombineFunc CombineFunc;
        const TFinalFunc FinalFunc;
        const ui32 StateColumn;
        const NPg::TAggregateDesc& AggDesc;
    };

public:
    std::unique_ptr<NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorCombineAll>> PrepareCombineAll(
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const NPg::TAggregateDesc& aggDesc) const {
        return std::make_unique<TPreparedCombineAllAggregator>(TransFunc, SerializeFunc, argsColumns, filterColumn, aggDesc);
    }

    std::unique_ptr<NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorCombineKeys>> PrepareCombineKeys(
        const std::vector<ui32>& argsColumns,
        const NPg::TAggregateDesc& aggDesc) {
        return std::make_unique<TPreparedCombineKeysAggregator>(TransFunc, SerializeFunc, argsColumns, aggDesc);
    }

    std::unique_ptr<NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
        ui32 stateColumn,
        const NPg::TAggregateDesc& aggDesc) {
        return std::make_unique<TPreparedFinalizeKeysAggregator>(DeserializeFunc, CombineFunc, FinalFunc, stateColumn, aggDesc);
    }

private:
    const TTransFunc TransFunc;
    const TCombineFunc CombineFunc;
    const TSerializeFunc SerializeFunc;
    const TDeserializeFunc DeserializeFunc;
    const TFinalFunc FinalFunc;
};

#if defined(_tsan_enabled_) || defined(_msan_enabled_) || defined(_asan_enabled_) || !defined(NDEBUG)
#ifndef USE_SLOW_PG_KERNELS
#define USE_SLOW_PG_KERNELS
#endif
#endif

TExecFunc FindExec(Oid oid);

const NPg::TAggregateDesc& ResolveAggregation(const TString& name, NKikimr::NMiniKQL::TTupleType* tupleType, const std::vector<ui32>& argsColumns, NKikimr::NMiniKQL::TType* returnType);

}

