#pragma once

#include <ydb/library/yql/public/udf/arrow/block_reader.h>
#include <ydb/library/yql/public/udf/arrow/block_builder.cpp>
#include <arrow/compute/kernel.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

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

template <typename TFunc, bool IsStrict, bool IsFixedResult, bool HasScalars, bool HasNulls, typename TArgsPolicy, typename TBuilder>
void GenericExecImpl(const arrow::compute::ExecBatch& batch, size_t length, const TPgKernelState& state, TBuilder& builder, FunctionCallInfo fcinfo) {
    for (size_t i = 0; i < length; ++i) {
        Datum ret;
        if constexpr (!TArgsPolicy::VarArgs) {
            if (!constexpr_for_tuple([&](auto const& j, auto const& v) {
                NullableDatum d;
                if (HasScalars && batch.values[j].is_scalar()) {
                    if (v) {
                        FillScalarItem<HasNulls, true>(*batch.values[j].scalar(), d);
                    } else {
                        FillScalarItem<HasNulls, false>(*batch.values[j].scalar(), d);
                    }
                } else {
                    if (v) {
                        FillArrayItem<HasNulls, true>(*batch.values[j].array(), i, d);
                    } else {
                        FillArrayItem<HasNulls, false>(*batch.values[j].array(), i, d);
                    }
                }

                if (HasNulls && IsStrict && d.isnull) {
                    return false;
                }

                fcinfo->args[j] = d;
                return true;            
            }, TArgsPolicy::IsFixedArg)) {
                goto SkipCall;
            }
        } else {
            for (size_t j = 0; j < batch.values.size(); ++i) {
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
                    builder.Add(NUdf::TBlockItem{});
                    goto SkipCall;
                }

                fcinfo->args[j] = d;            
            }
        }

        ret = TFunc()(fcinfo);
        if constexpr (IsFixedResult) {
            if (fcinfo->isnull) {
                builder.Add(NUdf::TBlockItem{});
            } else {
                builder.Add(NUdf::TBlockItem(ui64(ret)));
            }
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
}

template <typename TFunc, bool IsStrict, bool IsFixedResult, typename TArgsPolicy>
void GenericExecImpl2(bool hasScalars, bool hasNulls, arrow::compute::KernelContext* ctx,
    const arrow::compute::ExecBatch& batch, size_t length, const TPgKernelState& state,
    FunctionCallInfo fcinfo, arrow::Datum* res) {
    if (hasScalars) {
        if (hasNulls) {
            if constexpr (IsFixedResult) {
                NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                GenericExecImpl<TFunc, IsStrict, IsFixedResult, true, true, TArgsPolicy>(batch, length, state, builder, fcinfo);
                *res = builder.Build(true);
            } else {
                NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                GenericExecImpl<TFunc, IsStrict, IsFixedResult, true, true, TArgsPolicy>(batch, length, state, builder, fcinfo);
                *res = builder.Build(true);
            }
        } else {
            if constexpr (IsFixedResult) {
                NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                GenericExecImpl<TFunc, IsStrict, IsFixedResult, true, false, TArgsPolicy>(batch, length, state, builder, fcinfo);
                *res = builder.Build(true);
            } else {
                NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                GenericExecImpl<TFunc, IsStrict, IsFixedResult, true, false, TArgsPolicy>(batch, length, state, builder, fcinfo);
                *res = builder.Build(true);
            }
        }
    } else {
        if (hasNulls) {
            if constexpr (IsFixedResult) {
                NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                GenericExecImpl<TFunc, IsStrict, IsFixedResult, false, true, TArgsPolicy>(batch, length, state, builder, fcinfo);
                *res = builder.Build(true);
            } else {
                NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                GenericExecImpl<TFunc, IsStrict, IsFixedResult, false, true, TArgsPolicy>(batch, length, state, builder, fcinfo);
                *res = builder.Build(true);
            }
        } else {
            if constexpr (IsFixedResult) {
                NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                GenericExecImpl<TFunc, IsStrict, IsFixedResult, false, false, TArgsPolicy>(batch, length, state, builder, fcinfo);
                *res = builder.Build(true);
            } else {
                NUdf::TStringArrayBuilder<arrow::BinaryType, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *ctx->memory_pool(), length);
                GenericExecImpl<TFunc, IsStrict, IsFixedResult, false, false, TArgsPolicy>(batch, length, state, builder, fcinfo);
                *res = builder.Build(true);
            }
        }
    }
}

struct TDefaultArgsPolicy {
    static constexpr bool VarArgs = true;
    static constexpr std::array<bool, 0> IsFixedArg = {};
};

template <typename TFunc, bool IsStrict, bool IsFixedResult, typename TArgsPolicy = TDefaultArgsPolicy>
arrow::Status GenericExec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
    LOCAL_FCINFO(fcinfo, FUNC_MAX_ARGS);
    const auto& state = dynamic_cast<TPgKernelState&>(*ctx->state());
	fcinfo->flinfo = state.flinfo;
	fcinfo->context = state.context;
	fcinfo->resultinfo = state.resultinfo;
	fcinfo->fncollation = state.fncollation;
    fcinfo->nargs = batch.values.size();    
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
    GenericExecImpl2<TFunc, IsStrict, IsFixedResult, TArgsPolicy>(hasScalars, hasNulls, ctx, batch, length, state, fcinfo, res);
    return arrow::Status::OK();
}

typedef arrow::Status (*TExecFunc)(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res);

TExecFunc FindExec(Oid oid);
void RegisterExec(Oid oid, TExecFunc func);

}
