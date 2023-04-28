#pragma once

#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.cpp>
#include <arrow/compute/kernel.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>

extern "C" {
#include "postgres.h"
#include "fmgr.h"
}

namespace NYql {

struct TPgKernelState : arrow::compute::KernelState {
	FmgrInfo   *flinfo;			/* ptr to lookup info used for this call */
	fmNodePtr	context;		/* pass info about context of call */
	fmNodePtr	resultinfo;		/* pass or return extra info about result */
	Oid			fncollation;	/* collation for function to use */
    std::vector<bool> IsFixedArg;
    bool IsFixedResult;
    bool IsCStringResult;
};

template <Datum (*PgFunc)(FunctionCallInfo)>
struct TPgDirectFunc {
    Datum operator()(FunctionCallInfo info) {
        return PgFunc(info);
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

template <typename TFunc, bool IsStrict, bool IsFixedResult, bool HasScalars, bool HasNulls, typename TArgsPolicy, EScalarArgBinary ScalarArgBinary, typename TBuilder>
Y_NO_INLINE arrow::Datum GenericExecImpl(const arrow::compute::ExecBatch& batch, size_t length, const TPgKernelState& state, TBuilder& builder) {
    LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);    
	fcinfo->flinfo = state.flinfo;
	fcinfo->context = state.context;
	fcinfo->resultinfo = state.resultinfo;
	fcinfo->fncollation = state.fncollation;
    fcinfo->nargs = batch.values.size();    

    std::array<NullableDatum, TArgsPolicy::IsFixedArg.size()> scalars;
    std::array<bool, TArgsPolicy::IsFixedArg.size()> isScalar;
    std::array<ui64, TArgsPolicy::IsFixedArg.size()> offsets;
    std::array<const ui8*, TArgsPolicy::IsFixedArg.size()> validMasks;
    std::array<ui64, TArgsPolicy::IsFixedArg.size()> validOffsetMask;
    ui8 fakeValidByte = 0xFF;
    std::array<const ui64*, TArgsPolicy::IsFixedArg.size()> fixedArrays;
    std::array<const ui32*, TArgsPolicy::IsFixedArg.size()> stringOffsetsArrays;
    std::array<const ui8*, TArgsPolicy::IsFixedArg.size()> stringDataArrays;
    if constexpr (!TArgsPolicy::VarArgs) {
        for (size_t j = 0; j < TArgsPolicy::IsFixedArg.size(); ++j) {
            isScalar[j] = batch.values[j].is_scalar();
            if (isScalar[j]) {
                const auto& scalar = *batch.values[j].scalar();
                if (!scalar.is_valid) {
                    scalars[j].isnull = true;
                } else {
                    scalars[j].isnull = false;
                    if (TArgsPolicy::IsFixedArg[j]) {
                        scalars[j].value = (Datum)*static_cast<const ui64*>(arrow::internal::checked_cast<const arrow::internal::PrimitiveScalarBase&>(scalar).data());
                    } else {
                        auto buffer = arrow::internal::checked_cast<const arrow::BaseBinaryScalar&>(scalar).value;
                        scalars[j].value = (Datum)buffer->data();
                    }
                }
            } else {
                const auto& array = *batch.values[j].array();
                offsets[j] = array.offset;
                validMasks[j] = array.GetValues<ui8>(0, 0);
                if (validMasks[j]) {
                    validOffsetMask[j] = ~0ull;
                } else {
                    validOffsetMask[j] = 0ull;
                    validMasks[j] = &fakeValidByte;                    
                }
                if (TArgsPolicy::IsFixedArg[j]) {
                    fixedArrays[j] = array.GetValues<ui64>(1);
                } else {
                    stringOffsetsArrays[j] = array.GetValues<ui32>(1);
                    stringDataArrays[j] = array.GetValues<ui8>(2);
                }
            }
        }
    }

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
                    isScalar[j])) {
                    d = scalars[j];
                } else {
                    d.isnull = false;                    
                    if constexpr (HasNulls) {
                        ui64 fullIndex = (i + offsets[j]) & validOffsetMask[j];
                        d.isnull = ((validMasks[j][fullIndex >> 3] >> (fullIndex & 0x07)) & 1) == 0;
                    }

                    if (v) {
                        d.value = (Datum)fixedArrays[j][i];
                    } else {
                        d.value = (Datum)(stringOffsetsArrays[j][i] + stringDataArrays[j]);
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
        ret = TFunc()(fcinfo);
        if constexpr (IsFixedResult) {
            fixedResultData[i] = ui64(ret);
            fixedResultValidMask[i] = !fcinfo->isnull;
        } else {
            if (fcinfo->isnull) {
                builder.Add(NUdf::TBlockItem{});
            } else {
                auto ptr = (const char*)ret;
                auto len = state.IsCStringResult ? 1 + strlen(ptr) : VARSIZE((const text*)ptr);
                builder.Add(NUdf::TBlockItem(NUdf::TStringRef(ptr, len)));
            }
        }
SkipCall:;
    }

    return builder.Build(true);    
}

template <typename TFunc, bool IsStrict, bool IsFixedResult, bool HasScalars, bool HasNulls, typename TArgsPolicy, typename TBuilder>
Y_NO_INLINE arrow::Datum GenericExecImpl3(const arrow::compute::ExecBatch& batch, size_t length, const TPgKernelState& state, TBuilder& builder) {
    if constexpr (!TArgsPolicy::VarArgs) {
        if (TArgsPolicy::IsFixedArg.size() == 2) {
            if (batch.values[0].is_scalar()) {
                return GenericExecImpl<TFunc, IsStrict, IsFixedResult, HasScalars, HasNulls, TArgsPolicy, EScalarArgBinary::First, TBuilder>(batch, length, state, builder);                
            }

            if (batch.values[1].is_scalar()) {
                return GenericExecImpl<TFunc, IsStrict, IsFixedResult, HasScalars, HasNulls, TArgsPolicy, EScalarArgBinary::Second, TBuilder>(batch, length, state, builder);                
            }
        }
    }

    return GenericExecImpl<TFunc, IsStrict, IsFixedResult, HasScalars, HasNulls, TArgsPolicy, EScalarArgBinary::Unknown, TBuilder>(batch, length, state, builder);
}

template <typename TFunc, bool IsStrict, bool IsFixedResult, typename TArgsPolicy>
Y_NO_INLINE void GenericExecImpl2(bool hasScalars, bool hasNulls, arrow::compute::KernelContext* ctx,
    const arrow::compute::ExecBatch& batch, size_t length, const TPgKernelState& state, arrow::Datum* res) {
    if (hasScalars) {
        if (hasNulls) {
            if constexpr (IsFixedResult) {
                NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                *res = GenericExecImpl3<TFunc, IsStrict, IsFixedResult, true, true, TArgsPolicy>(batch, length, state, builder);
            } else {
                NUdf::TStringArrayBuilder<arrow::BinaryType, true, NUdf::EPgStringType::None> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                *res = GenericExecImpl3<TFunc, IsStrict, IsFixedResult, true, true, TArgsPolicy>(batch, length, state, builder);
            }
        } else {
            if constexpr (IsFixedResult) {
                NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                *res = GenericExecImpl3<TFunc, IsStrict, IsFixedResult, true, false, TArgsPolicy>(batch, length, state, builder);
            } else {
                NUdf::TStringArrayBuilder<arrow::BinaryType, true, NUdf::EPgStringType::None> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                *res = GenericExecImpl3<TFunc, IsStrict, IsFixedResult, true, false, TArgsPolicy>(batch, length, state, builder);
            }
        }
    } else {
        if (hasNulls) {
            if constexpr (IsFixedResult) {
                NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                *res = GenericExecImpl3<TFunc, IsStrict, IsFixedResult, false, true, TArgsPolicy>(batch, length, state, builder);
            } else {
                NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                *res = GenericExecImpl3<TFunc, IsStrict, IsFixedResult, false, true, TArgsPolicy>(batch, length, state, builder);
            }
        } else {
            if constexpr (IsFixedResult) {
                NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                *res = GenericExecImpl3<TFunc, IsStrict, IsFixedResult, false, false, TArgsPolicy>(batch, length, state, builder);
            } else {
                NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                *res = GenericExecImpl3<TFunc, IsStrict, IsFixedResult, false, false, TArgsPolicy>(batch, length, state, builder);
            }
        }
    }
}

struct TDefaultArgsPolicy {
    static constexpr bool VarArgs = true;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};

extern "C" TPgKernelState& GetPGKernelState(arrow::compute::KernelContext* ctx);

template <typename TFunc, bool IsStrict, bool IsFixedResult, typename TArgsPolicy = TDefaultArgsPolicy>
Y_NO_INLINE arrow::Status GenericExec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
    const auto& state = GetPGKernelState(ctx);
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
    Y_ENSURE(state.flinfo->fn_strict == IsStrict);
    Y_ENSURE(state.IsFixedResult == IsFixedResult);
    GenericExecImpl2<TFunc, IsStrict, IsFixedResult, TArgsPolicy>(hasScalars, hasNulls, ctx, batch, length, state, res);
    return arrow::Status::OK();
}

typedef arrow::Status (*TExecFunc)(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res);

TExecFunc FindExec(Oid oid);
void RegisterExec(Oid oid, TExecFunc func);

}
