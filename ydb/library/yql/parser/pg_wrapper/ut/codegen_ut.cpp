#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>
#include <ydb/library/yql/minikql/codegen/codegen.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>

#include <arrow/compute/kernel.h>
#include <arrow/array/builder_primitive.h>

#include <llvm/IR/Module.h>

#include <ydb/library/yql/parser/pg_wrapper/arrow.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/catalog/pg_collation_d.h>
#include <ydb/library/yql/parser/pg_catalog/catalog.h>

#include <util/datetime/cputimer.h>

using namespace NYql;
using namespace NYql::NCodegen;

extern "C" {
extern TExecFunc arrow_date_eq();
}

Y_UNIT_TEST_SUITE(TPgCodegen) {
    void PgFuncImpl(bool useBC) {
        TExecFunc execFunc;
        ICodegen::TPtr codegen;
        if (useBC) {
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
        } else {        
            execFunc = arrow_date_eq();
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
        arrow::Datum arg1(out), arg2(out);

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

    Y_UNIT_TEST(PgFunc) {
        PgFuncImpl(false);
    }

#if defined(NDEBUG) && !defined(_asan_enabled_)
    Y_UNIT_TEST(PgFuncBC) {
        PgFuncImpl(true);
    }
#endif
}
