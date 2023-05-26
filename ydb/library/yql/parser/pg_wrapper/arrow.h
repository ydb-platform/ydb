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
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
}

#include "utils.h"

namespace NYql {

struct TPgKernelState : arrow::compute::KernelState {
	FmgrInfo    flinfo;			/* lookup info used for this call */
	fmNodePtr	context;		/* pass info about context of call */
	fmNodePtr	resultinfo;		/* pass or return extra info about result */
	Oid			fncollation;	/* collation for function to use */
    TString     Name;
    std::vector<bool> IsFixedArg;
    bool IsFixedResult;
    bool IsCStringResult;
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
void SaveNullableDatum(const NullableDatum& from, NullableDatum& to, bool isCString) {
    bool wasNull = to.isnull;
    to.isnull = from.isnull;
    if (!to.isnull) {
        if constexpr (IsFixed) {
            to.value = from.value;
        } else {
            if (!wasNull) {
                if (to.value == from.value) {
                    return;
                }

                pfree((void*)to.value);
            }

            auto length = isCString ? strlen((const char*)from.value) : GetFullVarSize((const text*)from.value);
            to.value = (Datum)palloc(length);
            memcpy((void*)to.value, (const void*)from.value, length);
        }
    } else {
        if (!wasNull) {
            if constexpr (!IsFixed) {
                pfree((void*)to.value);
                to.value = 0;
            }
        }
    }
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

    void Bind(const std::vector<arrow::Datum>& values, size_t skipArgs = 0) {
        if constexpr (!TArgsPolicy::VarArgs) {
            Y_ENSURE(TArgsPolicy::IsFixedArg.size() == values.size() + skipArgs);
            for (size_t j = skipArgs; j < TArgsPolicy::IsFixedArg.size(); ++j) {
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
                            Scalars[j].value = (Datum)buffer->data();
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
                        StringDataArrays[j] = array.GetValues<ui8>(2);
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
            d.value = (Datum)item.AsStringRef().Data();
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
            d.value = (Datum)item.AsStringRef().Data();
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

extern "C" TPgKernelState& GetPGKernelState(arrow::compute::KernelContext* ctx);
extern "C" void WithPgTry(const TString& funcName, const std::function<void()>& func);

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
        WithPgTry(state.Name, [&]() {
            Dispatch1(hasScalars, hasNulls, ctx, batch, length, state, res);
        });

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
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true, NUdf::EPgStringType::None> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<true, true>(batch, length, state, builder);
                }
            } else {
                if constexpr (IsFixedResult) {
                    NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<true, false>(batch, length, state, builder);
                } else {
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true, NUdf::EPgStringType::None> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<true, false>(batch, length, state, builder);
                }
            }
        } else {
            if (hasNulls) {
                if constexpr (IsFixedResult) {
                    NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<false, true>(batch, length, state, builder);
                } else {
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<false, true>(batch, length, state, builder);
                }
            } else {
                if constexpr (IsFixedResult) {
                    NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                    *res = Dispatch2<false, false>(batch, length, state, builder);
                } else {
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
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
                            d.value = (Datum)(inputArgsAccessor.StringOffsetsArrays[j][i] + inputArgsAccessor.StringDataArrays[j]);
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
            ret = Func(fcinfo);
            if constexpr (IsFixedResult) {
                fixedResultData[i] = ui64(ret);
                fixedResultValidMask[i] = !fcinfo->isnull;
            } else {
                if (fcinfo->isnull) {
                    builder.Add(NUdf::TBlockItem{});
                } else {
                    auto ptr = (const char*)ret;
                    auto len = state.IsCStringResult ? 1 + strlen(ptr) : VARHDRSZ + VARSIZE((const text*)ptr);
                    builder.Add(NUdf::TBlockItem(NUdf::TStringRef(ptr, len)));
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
    class TCombineAllAggregator : public NKikimr::NMiniKQL::TCombineAllTag::TBase {
    public:
        using TBase = NKikimr::NMiniKQL::TCombineAllTag::TBase;
        TCombineAllAggregator(TTransFunc transFunc, TSerializeFunc serializeFunc, const std::vector<ui32>& argsColumns,
            const NPg::TAggregateDesc& aggDesc, NKikimr::NMiniKQL::TComputationContext& ctx)
            : TBase(sizeof(NullableDatum), std::optional<ui32>(), ctx)
            , TransFunc(transFunc)
            , SerializeFunc(serializeFunc)
            , ArgsColumns(argsColumns)
            , AggDesc(aggDesc)
        {
            if (!HasInitValue && IsTransStrict) {
                Y_ENSURE(AggDesc.ArgTypes.size() == 1);
            }

            const auto& transDesc = NPg::LookupProc(AggDesc.TransFuncId);
            for (ui32 i = 1; i < transDesc.ArgTypes.size(); ++i) {
                IsFixedArg.push_back(NPg::LookupType(transDesc.ArgTypes[i]).PassByValue);
            }

            Zero(TransFuncInfo);
            fmgr_info(AggDesc.TransFuncId, &TransFuncInfo);
            Y_ENSURE(TransFuncInfo.fn_addr);
            auto nargs = NPg::LookupProc(AggDesc.TransFuncId).ArgTypes.size();
            if constexpr (HasSerialize) {
                Zero(SerializeFuncInfo);
                fmgr_info(AggDesc.SerializeFuncId, &SerializeFuncInfo);
                Y_ENSURE(SerializeFuncInfo.fn_addr);
            }
            if constexpr (HasInitValue) {
                Zero(InFuncInfo);
                const auto& transTypeDesc = NPg::LookupType(AggDesc.TransTypeId);
                auto inFuncId = transTypeDesc.InFuncId;
                if (transTypeDesc.TypeId == transTypeDesc.ArrayTypeId) {
                    inFuncId = NPg::LookupProc("array_in", { 0,0,0 }).ProcId;
                }

                TypeIOParam = MakeTypeIOParam(transTypeDesc);
                fmgr_info(inFuncId, &InFuncInfo);
                Y_ENSURE(InFuncInfo.fn_addr);
            }
        }

    private:
        void DestroyState(void* state) noexcept final {
            auto typedState = (NullableDatum*)state;
            if constexpr (!IsTransTypeFixed) {
                if (!typedState->isnull) {
                    pfree((void*)typedState->value);
                }
            }
        }

        void InitState(void* state) final {
            new(state) NullableDatum();
            auto typedState = (NullableDatum*)state;
            typedState->isnull = true;
            typedState->value = 0;
            if constexpr (HasInitValue) {
                WithPgTry(AggDesc.Name, [&]() {
                    LOCAL_FCINFO(inCallInfo, 3);
                    inCallInfo->flinfo = &InFuncInfo;
                    inCallInfo->nargs = 3;
                    inCallInfo->fncollation = DEFAULT_COLLATION_OID;
                    inCallInfo->isnull = false;
                    inCallInfo->args[0] = { (Datum)AggDesc.InitValue.c_str(), false };
                    inCallInfo->args[1] = { ObjectIdGetDatum(TypeIOParam), false };
                    inCallInfo->args[2] = { Int32GetDatum(-1), false };

                    auto state = InFuncInfo.fn_addr(inCallInfo);
                    Y_ENSURE(!inCallInfo->isnull);
                    typedState->value = state;
                    typedState->isnull = false;
                });
            }
        }

        void AddMany(void* state, const NUdf::TUnboxedValue* columns, ui64 batchLength, std::optional<ui64> filtered) final {
            auto typedState = (NullableDatum*)state;
            std::vector<arrow::Datum> values;
            values.reserve(ArgsColumns.size());
            for (auto col : ArgsColumns) {
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

            WithPgTry(AggDesc.Name, [&]() {
                if (hasNulls) {
                    if (hasScalars) {
                        AddManyImpl<true, true>(typedState, values, batchLength);
                    } else {
                        AddManyImpl<true, false>(typedState, values, batchLength);
                    }
                } else {
                    if (hasScalars) {
                        AddManyImpl<false, true>(typedState, values, batchLength);
                    } else {
                        AddManyImpl<false, false>(typedState, values, batchLength);
                    }
                }
            });
        }

        template <bool HasNulls, bool HasScalars>
        void AddManyImpl(NullableDatum* typedState, const std::vector<arrow::Datum>& values, ui64 batchLength) {
            LOCAL_FCINFO(transCallInfo, FUNC_MAX_ARGS);
            transCallInfo->flinfo = &TransFuncInfo;
            transCallInfo->nargs = 1;
            transCallInfo->fncollation = DEFAULT_COLLATION_OID;
            transCallInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;

            transCallInfo->args[0] = *typedState;

            TInputArgsAccessor<TTransArgsPolicy> inputArgsAccessor;
            inputArgsAccessor.Bind(values, 1);

            for (ui64 i = 0; i < batchLength; ++i) {
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
                                d.value = (Datum)(inputArgsAccessor.StringOffsetsArrays[j][i] + inputArgsAccessor.StringDataArrays[j]);
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
                            if (IsFixedArg[j]) {
                                FillScalarItem<HasNulls, true>(*values[j].scalar(), d);
                            } else {
                                FillScalarItem<HasNulls, false>(*values[j].scalar(), d);
                            }
                        } else {
                            if (IsFixedArg[j]) {
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
                ret = TransFunc(transCallInfo);
                transCallInfo->args[0].value = ret;
                transCallInfo->args[0].isnull = transCallInfo->isnull;
SkipCall:;
            }

            SaveNullableDatum<IsTransTypeFixed>(transCallInfo->args[0], *typedState, AggDesc.TransTypeId == CSTRINGOID);
        }

        NUdf::TUnboxedValue FinishOne(const void* state) final {
            auto typedState = (NullableDatum*)state;
            if (typedState->isnull) {
                return {};
            }

            if constexpr (HasSerialize) {
                NUdf::TUnboxedValue ret;
                WithPgTry(AggDesc.Name, [&]() {
                    LOCAL_FCINFO(serializeCallInfo, 1);
                    serializeCallInfo->flinfo = &SerializeFuncInfo;
                    serializeCallInfo->nargs = 1;
                    serializeCallInfo->fncollation = DEFAULT_COLLATION_OID;
                    serializeCallInfo->context = (Node*)NKikimr::NMiniKQL::TlsAllocState->CurrentContext;
                    serializeCallInfo->isnull = false;
                    serializeCallInfo->args[0].isnull = false;
                    serializeCallInfo->args[0].value = typedState->value;
                    auto ser = SerializeFunc(serializeCallInfo);
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
                });

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

        const TTransFunc TransFunc;
        const TSerializeFunc SerializeFunc;
        const std::vector<ui32> ArgsColumns;
        const NPg::TAggregateDesc& AggDesc;
        std::vector<bool> IsFixedArg;
        bool IsTransTypeCString;
        FmgrInfo TransFuncInfo;
        FmgrInfo SerializeFuncInfo;
        FmgrInfo InFuncInfo;
        ui32 TypeIOParam = 0;
    };

    class TPreparedCombineAllAggregator : public NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorCombineAll>{
    public:
        TPreparedCombineAllAggregator(TTransFunc transFunc, TSerializeFunc serializeFunc, const std::vector<ui32>& argsColumns,
            const NPg::TAggregateDesc& aggDesc)
            : IPreparedBlockAggregator(sizeof(NullableDatum))
            , TransFunc(transFunc)
            , SerializeFunc(serializeFunc)
            , ArgsColumns(argsColumns)
            , AggDesc(aggDesc)
        {}
    private:

        std::unique_ptr<NKikimr::NMiniKQL::IBlockAggregatorCombineAll> Make(NKikimr::NMiniKQL::TComputationContext& ctx) const {
            return std::make_unique<TCombineAllAggregator>(TransFunc, SerializeFunc, ArgsColumns, AggDesc, ctx);
        }

        const TTransFunc TransFunc;
        const TSerializeFunc SerializeFunc;
        const std::vector<ui32> ArgsColumns;
        const NPg::TAggregateDesc& AggDesc;
    };

public:
    std::unique_ptr<NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorCombineAll>> PrepareCombineAll(
        std::optional<ui32> filterColumn,
        const std::vector<ui32>& argsColumns,
        const NPg::TAggregateDesc& aggDesc) const {
        Y_ENSURE(!filterColumn); // TODO
        return std::make_unique<TPreparedCombineAllAggregator>(TransFunc, SerializeFunc, argsColumns, aggDesc);
    }

    std::unique_ptr<NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorCombineKeys>> PrepareCombineKeys(
        const std::vector<ui32>& argsColumns,
        const NPg::TAggregateDesc& aggDesc) {
        ythrow yexception() << "Not implemented";
    }

    std::unique_ptr<NKikimr::NMiniKQL::IPreparedBlockAggregator<NKikimr::NMiniKQL::IBlockAggregatorFinalizeKeys>> PrepareFinalizeKeys(
        ui32 stateColumn,
        const NPg::TAggregateDesc& aggDesc) {
        ythrow yexception() << "Not implemented";
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
void RegisterExec(Oid oid, TExecFunc func);

const NPg::TAggregateDesc& ResolveAggregation(const TString& name, NKikimr::NMiniKQL::TTupleType* tupleType, const std::vector<ui32>& argsColumns, NKikimr::NMiniKQL::TType* returnType);

}
