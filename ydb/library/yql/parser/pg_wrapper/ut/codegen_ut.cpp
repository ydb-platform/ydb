#include "../pg_compat.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <ydb/library/yql/minikql/codegen/codegen.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <arrow/compute/kernel.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/builder_binary.h>

#include <llvm/IR/Module.h>

#include <ydb/library/yql/parser/pg_wrapper/arrow.h>

extern "C" {
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/catalog/pg_collation_d.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/fmgrprotos.h>
}

#include <ydb/library/yql/parser/pg_catalog/catalog.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>

#include <util/datetime/cputimer.h>

using namespace NYql;
using namespace NYql::NCodegen;

extern "C" {
Y_PRAGMA_DIAGNOSTIC_PUSH
Y_PRAGMA("GCC diagnostic ignored \"-Wreturn-type-c-linkage\"")
#include <ydb/library/yql/parser/pg_wrapper/pg_kernels_fwd.inc>
Y_PRAGMA_DIAGNOSTIC_POP
}

enum class EKernelFlavor {
    Indirect,
    DefArg,
    Cpp,
    BitCode,
    Ideal
};

Y_UNIT_TEST_SUITE(TPgCodegen) {
    void PgFuncImpl(EKernelFlavor flavor, bool constArg, bool fixed) {
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
            //codegen->ShowGeneratedFunctions(&Cerr);
            typedef TExecFunc (*TFunc)();
            auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
            execFunc = funcPtr();
            break;
        }
        case EKernelFlavor::Ideal: {
            if (fixed) {
                execFunc = [](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                    size_t length = batch.values[0].length();
                    //NUdf::TFixedSizeArrayBuilder<ui64, true> builder(NKikimr::NMiniKQL::TTypeInfoHelper(), arrow::uint64(), *arrow::default_memory_pool(), length);
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
                            //auto x = reader1.GetItem(array1, i).As<ui64>();
                            //auto y = reader2.GetItem(array2, i).As<ui64>();
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

                    *res = arrow::ArrayData::Make(arrow::uint64(), length ,{ data, nulls});
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
        state.ProcDesc = fixed ? &NPg::LookupProc("date_eq", { 0, 0 }) : &NPg::LookupProc("textout", { 0 });
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
            arrow::Datum arg1(out), arg2;
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

        {
            Cout << "begin...\n";            
            TSimpleTimer timer;
            for (size_t count = 0; count < (fixed ? 10000 : 1000); ++count) {
                arrow::Datum res;
                ARROW_OK(execFunc(&kernelCtx, batch, &res));
                Y_ENSURE(res.length() == N);
            }

            Cout << "done, elapsed: " << timer.Get() << "\n";
        }
    }

    Y_UNIT_TEST(PgFixedFuncIdeal) {
        PgFuncImpl(EKernelFlavor::Ideal, false, true);
        PgFuncImpl(EKernelFlavor::Ideal, true, true);
    }

    Y_UNIT_TEST(PgFixedFuncDefArg) {
        PgFuncImpl(EKernelFlavor::DefArg, false, true);
        PgFuncImpl(EKernelFlavor::DefArg, true, true);
    }

    Y_UNIT_TEST(PgFixedFuncIndirect) {
        PgFuncImpl(EKernelFlavor::Indirect, false, true);
        PgFuncImpl(EKernelFlavor::Indirect, true, true);
    }    

#if !defined(USE_SLOW_PG_KERNELS)
    Y_UNIT_TEST(PgFixedFuncCpp) {
        PgFuncImpl(EKernelFlavor::Cpp, false, true);
        PgFuncImpl(EKernelFlavor::Cpp, true, true);
    }

    Y_UNIT_TEST(PgFixedFuncBC) {
        PgFuncImpl(EKernelFlavor::BitCode, false, true);
        PgFuncImpl(EKernelFlavor::BitCode, true, true);
    }
#endif    

    Y_UNIT_TEST(PgStrFuncIdeal) {
        PgFuncImpl(EKernelFlavor::Ideal, false, false);
    }        

    Y_UNIT_TEST(PgStrFuncDefArg) {
        PgFuncImpl(EKernelFlavor::DefArg, false, false);
    }    

    Y_UNIT_TEST(PgStrFuncIndirect) {
        PgFuncImpl(EKernelFlavor::Indirect, false, false);
    }

#if !defined(USE_SLOW_PG_KERNELS)
    Y_UNIT_TEST(PgStrFuncCpp) {
        PgFuncImpl(EKernelFlavor::Cpp, false, false);
    }

    Y_UNIT_TEST(PgStrFuncBC) {
        PgFuncImpl(EKernelFlavor::BitCode, false, false);
    }
#endif

}
