#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <ydb/library/yql/minikql/codegen/codegen.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <arrow/compute/kernel.h>
#include <arrow/array/builder_primitive.h>

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
extern TExecFunc arrow_date_eq();
}

enum class EKernelFlavor {
    DefArg,
    Cpp,
    BitCode,
    Ideal
};

Y_UNIT_TEST_SUITE(TPgCodegen) {
    void PgFuncImpl(EKernelFlavor flavor, bool constArg) {
        TExecFunc execFunc;
        ICodegen::TPtr codegen;
        switch (flavor) {
        case EKernelFlavor::DefArg: {
            execFunc = &GenericExec<TPgDirectFunc<&date_eq>, true, true, TDefaultArgsPolicy>;
            break;
        }
        case EKernelFlavor::Cpp: {
            execFunc = arrow_date_eq();
            break;
        }
        case EKernelFlavor::BitCode: {
            codegen = ICodegen::Make(ETarget::Native);
            auto bitcode = NResource::Find("/llvm_bc/PgFuncs1");
            codegen->LoadBitCode(bitcode, "Funcs");
            auto func = codegen->GetModule().getFunction("arrow_date_eq");
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

            break;
        }
        }

        Y_ENSURE(execFunc);
        arrow::compute::ExecContext execContent;
        arrow::compute::KernelContext kernelCtx(&execContent);
        TPgKernelState state;
        kernelCtx.SetState(&state);
        FmgrInfo finfo;
        Zero(finfo);
        fmgr_info(NPg::LookupProc("date_eq", { 0, 0}).ProcId, &finfo);
        state.flinfo = &finfo;
        state.context = nullptr;
        state.resultinfo = nullptr;
        state.fncollation = DEFAULT_COLLATION_OID;
        state.IsCStringResult = false;
        state.IsFixedResult = true;
        state.IsFixedArg.push_back(true);
        state.IsFixedArg.push_back(true);
        const size_t N = 10000;
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

        {
            Cout << "begin...\n";            
            TSimpleTimer timer;
            for (size_t count = 0; count < 10000; ++count) {
                arrow::compute::ExecBatch batch({ arg1, arg2}, N);
                arrow::Datum res;
                ARROW_OK(execFunc(&kernelCtx, batch, &res));
                Y_ENSURE(res.is_array());
                Y_ENSURE(res.array()->length == N);
            }

            Cout << "done, elapsed: " << timer.Get() << "\n";
        }
    }

    Y_UNIT_TEST(PgFuncIdeal) {
        PgFuncImpl(EKernelFlavor::Ideal, false);
        PgFuncImpl(EKernelFlavor::Ideal, true);
    }

    Y_UNIT_TEST(PgFuncCpp) {
        PgFuncImpl(EKernelFlavor::Cpp, false);
        PgFuncImpl(EKernelFlavor::Cpp, true);
    }

    Y_UNIT_TEST(PgFuncDefArg) {
        PgFuncImpl(EKernelFlavor::DefArg, false);
        PgFuncImpl(EKernelFlavor::DefArg, true);
    }    

#if defined(NDEBUG) && !defined(_asan_enabled_)
    Y_UNIT_TEST(PgFuncBC) {
        PgFuncImpl(EKernelFlavor::BitCode, false);
        PgFuncImpl(EKernelFlavor::BitCode, true);
    }
#endif
}
