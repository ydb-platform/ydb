#include "../pg_compat.h"

#include <benchmark/benchmark.h>
#include <library/cpp/resource/resource.h>
#include <yql/essentials/minikql/codegen/codegen.h>
#include <yql/essentials/minikql/arrow/arrow_defs.h>

#include <arrow/compute/kernel.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_binary.h>

#include <llvm/IR/Module.h>

#include <yql/essentials/parser/pg_wrapper/arrow.h>

extern "C" {
#include <yql/essentials/parser/pg_wrapper/postgresql/src/backend/catalog/pg_collation_d.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/backend/utils/fmgrprotos.h>
}

#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/minikql/arrow/arrow_util.h>

using namespace NYql;
using namespace NYql::NCodegen;

extern "C" {
    Y_PRAGMA_DIAGNOSTIC_PUSH
    Y_PRAGMA("GCC diagnostic ignored \"-Wreturn-type-c-linkage\"")
#include <yql/essentials/parser/pg_wrapper/pg_kernels_fwd.inc>
    Y_PRAGMA_DIAGNOSTIC_POP
}

enum class EKernelFlavor {
    Indirect,
    DefArg,
    Cpp,
    BitCode,
    Ideal
};

namespace {

void PgFuncImpl(benchmark::State& benchmarkState, EKernelFlavor flavor, bool constArg, bool fixed) {
    const TString& name = fixed ? "date_eq" : "textout";
    ICodegen::TPtr codegen;
    TExecFunc execFunc;
    switch (flavor) {
        case EKernelFlavor::Indirect: {
            if (fixed) {
                execFunc = MakeIndirectExec<true, true>(&date_eq);
            } else {
                execFunc = MakeIndirectExec<true, false>(&textout);
            }
            break;
        }
        case EKernelFlavor::DefArg: {
            if (fixed) {
                execFunc = TGenericExec<TPgDirectFunc<&date_eq>, true, true, TDefaultArgsPolicy>({});
            } else {
                execFunc = TGenericExec<TPgDirectFunc<&textout>, true, false, TDefaultArgsPolicy>({});
            }
            break;
        }
        case EKernelFlavor::Cpp: {
            execFunc = fixed ? arrow_date_eq() : arrow_textout();
            break;
        }
        case EKernelFlavor::BitCode: {
            codegen = ICodegen::Make(ETarget::Native);
            auto bitcode = NResource::Find(fixed ? "/llvm_bc/PgFuncs1" : "/llvm_bc/PgFuncs17");
            codegen->LoadBitCode(bitcode, "Funcs");
            auto func = codegen->GetModule().getFunction(std::string("arrow_" + name));
            Y_ENSURE(func);
            codegen->AddGlobalMapping("GetPGKernelState", (const void*)&GetPGKernelState);
            codegen->Verify();
            codegen->ExportSymbol(func);
            codegen->Compile();
            using TFunc = TExecFunc (*)();
            auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
            execFunc = funcPtr();
            break;
        }
        case EKernelFlavor::Ideal: {
            if (fixed) {
                execFunc = [](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                    size_t length = batch.values[0].length();
                    NUdf::TTypedBufferBuilder<ui64> dataBuilder(arrow::default_memory_pool());
                    NUdf::TTypedBufferBuilder<ui8> nullBuilder(arrow::default_memory_pool());
                    dataBuilder.Reserve(length);
                    nullBuilder.Reserve(length);
                    auto out = dataBuilder.MutableData();
                    auto outNulls = nullBuilder.MutableData();
                    NUdf::TFixedSizeBlockReader<ui64, false> reader1;
                    NUdf::TFixedSizeBlockReader<ui64, false> reader2;
                    const auto& array1 = *batch.values[0].array();
                    const auto ptr1 = array1.GetValues<ui64>(1);
                    if (batch.values[1].is_array()) {
                        const auto& array2 = *batch.values[1].array();
                        const auto ptr2 = array2.GetValues<ui64>(1);
                        for (size_t i = 0; i < length; ++i) {
                            auto x = ptr1[i];
                            auto y = ptr2[i];
                            out[i] = x == y ? 1 : 0;
                            outNulls[i] = false;
                        }
                    } else {
                        ui64 yConst = reader2.GetScalarItem(*batch.values[1].scalar()).As<ui64>();
                        for (size_t i = 0; i < length; ++i) {
                            auto x = ptr1[i];
                            out[i] = x == yConst ? 1 : 0;
                            outNulls[i] = false;
                        }
                    }

                    std::shared_ptr<arrow::Buffer> nulls;
                    nulls = nullBuilder.Finish();
                    nulls = NUdf::MakeDenseBitmap(nulls->data(), length, arrow::default_memory_pool());
                    std::shared_ptr<arrow::Buffer> data = dataBuilder.Finish();

                    *res = arrow::ArrayData::Make(arrow::uint64(), length, {data, nulls});
                    return arrow::Status::OK();
                };
            } else {
                execFunc = [](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                    size_t length = batch.values[0].length();
                    NUdf::TStringArrayBuilder<arrow::BinaryType, true, NUdf::EPgStringType::None> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::binary(), *ctx->memory_pool(), length);
                    NUdf::TStringBlockReader<arrow::BinaryType, true> reader;
                    const auto& array = *batch.values[0].array();
                    for (size_t i = 0; i < length; ++i) {
                        auto item = reader.GetItem(array, i);
                        if (!item) {
                            builder.Add(NUdf::TBlockItem{});
                        } else {
                            auto s = item.AsStringRef();
                            size_t len = s.Size() - VARHDRSZ - sizeof(void*);
                            const char* ptr = s.Data() + VARHDRSZ + sizeof(void*);
                            builder.Add(NUdf::TBlockItem{NUdf::TStringRef(ptr, len)});
                        }
                    }

                    *res = builder.Build(true);
                    return arrow::Status::OK();
                };
            };

            break;
        }
    }

    Y_ENSURE(execFunc);
    arrow::compute::ExecContext execContent;
    arrow::compute::KernelContext kernelCtx(&execContent);
    TPgKernelState state;
    kernelCtx.SetState(&state);
    FmgrInfo finfo;
    Zero(state.flinfo);
    state.ProcDesc = fixed ? &NPg::LookupProc("date_eq", {0, 0}) : &NPg::LookupProc("textout", {0});
    fmgr_info(state.ProcDesc->ProcId, &state.flinfo);
    state.context = nullptr;
    state.resultinfo = nullptr;
    state.fncollation = DEFAULT_COLLATION_OID;
    state.Name = name;
    if (fixed) {
        state.TypeLen = 1;
        state.IsFixedResult = true;
        state.IsFixedArg.push_back(true);
        state.IsFixedArg.push_back(true);
    } else {
        state.TypeLen = -2;
        state.IsFixedResult = false;
        state.IsFixedArg.push_back(false);
    }

#ifdef NDEBUG
    const size_t N = 10000;
#else
    const size_t N = 1000;
#endif
    std::vector<arrow::Datum> batchArgs;
    if (fixed) {
        arrow::UInt64Builder builder;
        ARROW_OK(builder.Reserve(N));
        for (size_t i = 0; i < N; ++i) {
            builder.UnsafeAppend(i);
        }

        std::shared_ptr<arrow::ArrayData> out;
        ARROW_OK(builder.FinishInternal(&out));
        arrow::Datum arg1(out);
        arrow::Datum arg2;
        if (constArg) {
            Cout << "with const arg\n";
            arg2 = NKikimr::NMiniKQL::MakeScalarDatum<ui64>(0);
        } else {
            arg2 = out;
        }

        batchArgs.push_back(arg1);
        batchArgs.push_back(arg2);
    } else {
        arrow::BinaryBuilder builder;
        ARROW_OK(builder.Reserve(N));
        for (size_t i = 0; i < N; ++i) {
            std::string s(sizeof(void*) + VARHDRSZ + 500, 'A' + i % 26);
            NUdf::ZeroMemoryContext(s.data() + sizeof(void*));
            auto t = (text*)(s.data() + sizeof(void*));
            SET_VARSIZE(t, VARHDRSZ + 500);
            ARROW_OK(builder.Append(s));
        }

        std::shared_ptr<arrow::ArrayData> out;
        ARROW_OK(builder.FinishInternal(&out));
        arrow::Datum arg1(out);
        batchArgs.push_back(arg1);
    }

    arrow::compute::ExecBatch batch(std::move(batchArgs), N);

    for (auto _ : benchmarkState) {
        for (size_t count = 0; count < (fixed ? 10000 : 1000); ++count) {
            arrow::Datum res;
            ARROW_OK(execFunc(&kernelCtx, batch, &res));
            Y_ENSURE(res.length() == N);
        }
    }
}

BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncIdeal_Vector, EKernelFlavor::Ideal, false, true);
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncIdeal_Scalar, EKernelFlavor::Ideal, true, true);
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncDefArg_Vector, EKernelFlavor::DefArg, false, true);
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncDefArg_Scalar, EKernelFlavor::DefArg, true, true);
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncIndirect_Vector, EKernelFlavor::Indirect, false, true);
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncIndirect_Scalar, EKernelFlavor::Indirect, true, true);

#if !defined(USE_SLOW_PG_KERNELS)
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncCpp_Vector, EKernelFlavor::Cpp, false, true);
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncCpp_Scalar, EKernelFlavor::Cpp, true, true);
    #if defined(YQL_USE_PG_BC)
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncBC_Vector, EKernelFlavor::BitCode, false, true);
BENCHMARK_CAPTURE(PgFuncImpl, PgFixedFuncBC_Scalar, EKernelFlavor::BitCode, true, true);
    #endif
#endif

BENCHMARK_CAPTURE(PgFuncImpl, PgStrFuncIdeal, EKernelFlavor::Ideal, false, false);
BENCHMARK_CAPTURE(PgFuncImpl, PgStrFuncDefArg, EKernelFlavor::DefArg, false, false);
BENCHMARK_CAPTURE(PgFuncImpl, PgStrFuncIndirect, EKernelFlavor::Indirect, false, false);
#if !defined(USE_SLOW_PG_KERNELS)
BENCHMARK_CAPTURE(PgFuncImpl, PgStrFuncCpp, EKernelFlavor::Cpp, false, false);
    #if defined(YQL_USE_PG_BC)
BENCHMARK_CAPTURE(PgFuncImpl, PgStrFuncBC, EKernelFlavor::BitCode, false, false);
    #endif
#endif

} // namespace
