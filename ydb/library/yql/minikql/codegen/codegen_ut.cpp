#include <ydb/library/yql/minikql/codegen/codegen.h>

#include <codegen_ut_llvm_deps.h> // Y_IGNORE

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/resource/resource.h>

using namespace NYql::NCodegen;
using namespace llvm;

extern "C" int mul(int x, int y) {
    return x * y;
}

extern "C" int sum(int x, int y) {
    return x + y;
}

namespace {

struct T128 {
    ui64 Lo;
    ui64 Hi;

    T128(ui64 x)
        : Lo(x)
        , Hi(0)
    {}

    bool operator==(const T128& other) const {
        return Lo == other.Lo && Hi == other.Hi;
    }
};

Function *CreateFibFunction(Module &M, LLVMContext &Context) {
    const auto funcType = FunctionType::get(Type::getInt32Ty(Context), {Type::getInt32Ty(Context)}, false);

    // Create the fib function and insert it into module M. This function is said
    // to return an int and take an int parameter.
    Function *FibF = cast<Function>(M.getOrInsertFunction("fib", funcType).getCallee());

    // Add a basic block to the function.
    BasicBlock *BB = BasicBlock::Create(Context, "EntryBlock", FibF);

    // Get pointers to the constants.
    Value *One = ConstantInt::get(Type::getInt32Ty(Context), 1);
    Value *Two = ConstantInt::get(Type::getInt32Ty(Context), 2);

    // Get pointer to the integer argument of the add1 function...
    auto ArgX = FibF->arg_begin();   // Get the arg.
    ArgX->setName("AnArg");            // Give it a nice symbolic name for fun.

                                        // Create the true_block.
    BasicBlock *RetBB = BasicBlock::Create(Context, "return", FibF);
    // Create an exit block.
    BasicBlock* RecurseBB = BasicBlock::Create(Context, "recurse", FibF);

    // Create the "if (arg <= 2) goto exitbb"
    Value *CondInst = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, &*ArgX, Two, "cond", BB);
    BranchInst::Create(RetBB, RecurseBB, CondInst, BB);

    // Create: ret int 1
    ReturnInst::Create(Context, One, RetBB);

    // create fib(x-1)
    Value *Sub = BinaryOperator::CreateSub(&*ArgX, One, "arg", RecurseBB);
    CallInst *CallFibX1 = CallInst::Create(FibF, Sub, "fibx1", RecurseBB);
    CallFibX1->setTailCall();

    // create fib(x-2)
    Sub = BinaryOperator::CreateSub(&*ArgX, Two, "arg", RecurseBB);
    CallInst *CallFibX2 = CallInst::Create(FibF, Sub, "fibx2", RecurseBB);
    CallFibX2->setTailCall();


    // fib(x-1)+fib(x-2)
    Value *Sum = BinaryOperator::CreateAdd(CallFibX1, CallFibX2,
        "addresult", RecurseBB);

    // Create the return instruction and add it to the basic block
    ReturnInst::Create(Context, Sum, RecurseBB);

    return FibF;
}

Function *CreateBadFibFunction(Module &M, LLVMContext &Context) {
    const auto funcType = FunctionType::get(Type::getInt32Ty(Context), {Type::getInt32Ty(Context)}, false);

    // Create the fib function and insert it into module M. This function is said
    // to return an int and take an int parameter.
    Function *FibF = cast<Function>(M.getOrInsertFunction("bad_fib", funcType).getCallee());

    BasicBlock *BB = BasicBlock::Create(Context, "EntryBlock", FibF);

    // Get pointers to the constants.
    Value *One = ConstantInt::get(Type::getInt64Ty(Context), 1);

    ReturnInst::Create(Context, One, BB);
    return FibF;
}

Function *CreateMulFunction(Module &M, LLVMContext &Context) {
    const auto funcType = FunctionType::get(Type::getInt32Ty(Context), {Type::getInt32Ty(Context), Type::getInt32Ty(Context)}, false);

    Function *MulF = cast<Function>(M.getOrInsertFunction("mul", funcType).getCallee());

    // Add a basic block to the function.
    BasicBlock *BB = BasicBlock::Create(Context, "EntryBlock", MulF);
    auto args = MulF->arg_begin();
    auto ArgX = args;   // Get the arg 1.
    ArgX->setName("x");
    auto ArgY = ++args;   // Get the arg 2.
    ArgY->setName("y");

    // arg1 * arg2
    Value *Mul = BinaryOperator::CreateMul(&*ArgX, &*ArgY, "res", BB);

    // Create the return instruction and add it to the basic block
    ReturnInst::Create(Context, Mul, BB);

    return MulF;
}

Function *CreateUseNativeFunction(Module &M, LLVMContext &Context) {
    const auto funcType = FunctionType::get(Type::getInt32Ty(Context), {Type::getInt32Ty(Context), Type::getInt32Ty(Context)}, false);

    Function *func = cast<Function>(M.getOrInsertFunction("add", funcType).getCallee());

    // Add a basic block to the function.
    BasicBlock *BB = BasicBlock::Create(Context, "EntryBlock", func);
    auto args = func->arg_begin();
    auto ArgX = args;   // Get the arg 1.
    ArgX->setName("x");
    auto ArgY = ++args;   // Get the arg 2.
    ArgY->setName("y");

    Function* func_mul = M.getFunction("mul");
    if (!func_mul) {
        func_mul = Function::Create(
        /*Type=*/FunctionType::get(Type::getInt32Ty(Context), {Type::getInt32Ty(Context), Type::getInt32Ty(Context)}, false),
        /*Linkage=*/GlobalValue::ExternalLinkage,
        /*Name=*/"mul", &M); // (external, no body)
        func_mul->setCallingConv(CallingConv::C);
    }

    // arg1 * arg2
    Value *Mul = CallInst::Create(func_mul, {&*ArgX, &*ArgY}, "res", BB);

    // Create the return instruction and add it to the basic block
    ReturnInst::Create(Context, Mul, BB);
    return func;
}

Function *CreateUseExternalFromGeneratedFunction(Module& main, LLVMContext &Context) {
    const auto funcType = FunctionType::get(Type::getInt32Ty(Context), {Type::getInt32Ty(Context), Type::getInt32Ty(Context), Type::getInt32Ty(Context)}, false);

    Function *func = cast<Function>(main.getOrInsertFunction("sum_sqr_3", funcType).getCallee());

    // Add a basic block to the function.
    BasicBlock *BB = BasicBlock::Create(Context, "EntryBlock", func);
    auto args = func->arg_begin();
    auto ArgX = args;   // Get the arg 1.
    ArgX->setName("x");
    auto ArgY = ++args;   // Get the arg 2.
    ArgY->setName("y");
    auto ArgZ = ++args;   // Get the arg 3.
    ArgZ->setName("z");

    Function* sum_sqr = main.getFunction("sum_sqr");

    Value *tmp = CallInst::Create(sum_sqr, {&*ArgX, &*ArgY}, "tmp", BB);
    Value *res = CallInst::Create(sum_sqr, {&*ArgZ, tmp}, "res", BB);

    // Create the return instruction and add it to the basic block
    ReturnInst::Create(Context, res, BB);
    return func;
}

Function *CreateUseExternalFromGeneratedFunction128(const ICodegen::TPtr& codegen, bool ir) {
    Module& main = codegen->GetModule();
    LLVMContext &Context = codegen->GetContext();
    auto typeInt128 = Type::getInt128Ty(Context);
    auto pointerInt128 = PointerType::getUnqual(typeInt128);
    const auto funcType = codegen->GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
        FunctionType::get(typeInt128, {typeInt128, typeInt128, typeInt128}, false):
        FunctionType::get(Type::getVoidTy(Context), {pointerInt128, pointerInt128, pointerInt128, pointerInt128}, false);

    Function *func = cast<Function>(main.getOrInsertFunction("sum_sqr_3", funcType).getCallee());

    auto args = func->arg_begin();

    // Add a basic block to the function.
    BasicBlock *BB = BasicBlock::Create(Context, "EntryBlock", func);
    llvm::Argument* retArg = nullptr;
    if (codegen->GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows) {
        retArg = &*args++;
        retArg->addAttr(Attribute::StructRet);
        retArg->addAttr(Attribute::NoAlias);
    }

    auto ArgX = args++;   // Get the arg 1.
    ArgX->setName("x");
    auto ArgY = args++;   // Get the arg 2.
    ArgY->setName("y");
    auto ArgZ = args++;   // Get the arg 3.
    ArgZ->setName("z");

    const auto type = FunctionType::get(Type::getVoidTy(Context), { pointerInt128, pointerInt128, pointerInt128 }, false);
    const auto sum_sqr = main.getOrInsertFunction(ir ? "sum_sqr_128_ir" : "sum_sqr_128", type);

    if (codegen->GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows) {
        Value* tmp1 = new AllocaInst(typeInt128, 0U, nullptr, llvm::Align(16), "tmp1", BB);
        Value* tmp2 = new AllocaInst(typeInt128, 0U, nullptr, llvm::Align(16), "tmp2", BB);
        CallInst::Create(sum_sqr, { &*tmp1, &*ArgX, &*ArgY }, "", BB);
        CallInst::Create(sum_sqr, { &*tmp2, &*ArgZ, &*tmp1 }, "", BB);
        auto res = new LoadInst(typeInt128, tmp2, "load_res", BB);
        new StoreInst(res, retArg, BB);
        // Create the return instruction and add it to the basic block
        ReturnInst::Create(Context, BB);
    } else {
        Value* tmp1 = new AllocaInst(typeInt128, 0U, nullptr, llvm::Align(16), "tmp1", BB);
        Value* tmp2 = new AllocaInst(typeInt128, 0U, nullptr, llvm::Align(16), "tmp2", BB);
        Value* argXPtr = new AllocaInst(typeInt128, 0U, nullptr, llvm::Align(16), "argXptr", BB);
        Value* argYPtr = new AllocaInst(typeInt128, 0U, nullptr, llvm::Align(16), "argYptr", BB);
        Value* argZPtr = new AllocaInst(typeInt128, 0U, nullptr, llvm::Align(16), "argZptr", BB);
        new StoreInst(&*ArgX, argXPtr, BB);
        new StoreInst(&*ArgY, argYPtr, BB);
        new StoreInst(&*ArgZ, argZPtr, BB);

        CallInst::Create(sum_sqr, { &*tmp1, &*argXPtr, &*argYPtr }, "", BB);
        CallInst::Create(sum_sqr, { &*tmp2, &*argZPtr, &*tmp1 }, "", BB);
        auto res = new LoadInst(typeInt128, tmp2, "load_res", BB);

        // Create the return instruction and add it to the basic block
        ReturnInst::Create(Context, res, BB);
    }
    return func;
}

}

#if !defined(_ubsan_enabled_) && !defined(HAVE_VALGRIND)
Y_UNIT_TEST_SUITE(TCodegenTests) {

    Y_UNIT_TEST(FibNative) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto func = CreateFibFunction(codegen->GetModule(), codegen->GetContext());
        codegen->Verify();
        codegen->Compile();
        typedef int(*TFunc)(int);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(24), 46368);
    }

    Y_UNIT_TEST(FibCurrentOS) {
        auto codegen = ICodegen::Make(ETarget::CurrentOS);
        auto func = CreateFibFunction(codegen->GetModule(), codegen->GetContext());
        codegen->Verify();
        codegen->Compile();
        typedef int(*TFunc)(int);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(24), 46368);
    }

    Y_UNIT_TEST(BadFib) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto func = CreateBadFibFunction(codegen->GetModule(), codegen->GetContext());
        Y_UNUSED(func);
        UNIT_ASSERT_EXCEPTION(codegen->Verify(), yexception);
    }

    Y_UNIT_TEST(FibFromBitCode) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto bitcode = NResource::Find("/llvm_bc/Funcs");
        codegen->LoadBitCode(bitcode, "Funcs");
        auto func = codegen->GetModule().getFunction("fib");
        codegen->Verify();
        codegen->ExportSymbol(func);
        codegen->Compile();
        typedef int(*TFunc)(int);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(24), 46368);
    }

    Y_UNIT_TEST(LinkWithNativeFunction) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto bitcode = NResource::Find("/llvm_bc/Funcs");
        codegen->LoadBitCode(bitcode, "Funcs");
        auto func = codegen->GetModule().getFunction("sum_sqr");
        codegen->AddGlobalMapping("mul", (void*)&sum);
        codegen->ExportSymbol(func);
        codegen->Verify();
        codegen->Compile();
        typedef int(*TFunc)(int, int);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(3, 4), 14);
    }

    Y_UNIT_TEST(LinkWithGeneratedFunction) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto mulFunc = CreateMulFunction(codegen->GetModule(), codegen->GetContext());
        Y_UNUSED(mulFunc);
        auto bitcode = NResource::Find("/llvm_bc/Funcs");
        codegen->LoadBitCode(bitcode, "Funcs");
        auto func = codegen->GetModule().getFunction("sum_sqr");
        codegen->ExportSymbol(func);
        codegen->Verify();
        codegen->Compile();
        typedef int(*TFunc)(int, int);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(3, 4), 25);
    }

    Y_UNIT_TEST(ReuseExternalCode) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto bitcode = NResource::Find("/llvm_bc/Funcs");
        codegen->LoadBitCode(bitcode, "Funcs");
        auto func = codegen->GetModule().getFunction("sum_sqr2");
        codegen->ExportSymbol(func);
        codegen->Verify();
        codegen->Compile();
        typedef int(*TFunc)(int, int);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(3, 4), 25);
    }

    Y_UNIT_TEST(UseObjectReference) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto bitcode = NResource::Find("/llvm_bc/Funcs");
        codegen->LoadBitCode(bitcode, "Funcs");
        auto func = codegen->GetModule().getFunction("str_size");
        codegen->ExportSymbol(func);
        codegen->Verify();
        codegen->Compile();
        typedef size_t(*TFunc)(const std::string&);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        const std::string hw("Hello World!");
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(hw), 12);
    }

    Y_UNIT_TEST(UseNativeFromGeneratedFunction) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto func = CreateUseNativeFunction(codegen->GetModule(), codegen->GetContext());
        codegen->AddGlobalMapping("mul", (void*)&mul);
        codegen->ExportSymbol(func);
        codegen->Verify();
        codegen->Compile();
        typedef int(*TFunc)(int, int);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(3, 4), 12);
    }

    Y_UNIT_TEST(UseExternalFromGeneratedFunction) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto bitcode = NResource::Find("/llvm_bc/Funcs");
        codegen->LoadBitCode(bitcode, "Funcs");
        auto func = CreateUseExternalFromGeneratedFunction(codegen->GetModule(), codegen->GetContext());
        codegen->ExportSymbol(func);
        codegen->AddGlobalMapping("mul", (void*)&mul);
        codegen->Verify();
        codegen->Compile();
        typedef int(*TFunc)(int, int, int);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT_VALUES_EQUAL(funcPtr(7, 4, 8), 4289);
    }

    Y_UNIT_TEST(UseExternalFromGeneratedFunction_128bit_Compiled) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto bitcode = NResource::Find("/llvm_bc/Funcs");
        codegen->LoadBitCode(bitcode, "Funcs");
        auto func = CreateUseExternalFromGeneratedFunction128(codegen, false);
        codegen->ExportSymbol(func);
        codegen->Verify();
        codegen->Compile();
        TStringStream str;
        codegen->ShowGeneratedFunctions(&str);
#ifdef _win_
        typedef T128 (*TFunc)(T128, T128, T128);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT(funcPtr(T128(7), T128(4), T128(8)) == T128(4289));
#else
        typedef unsigned __int128(*TFunc)(__int128, __int128, __int128);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT(funcPtr(7, 4, 8) == 4289);
#endif
#if !defined(_asan_enabled_) && !defined(_msan_enabled_) && !defined(_tsan_enabled_) && !defined(_hardening_enabled_)
        if (str.Str().Contains("call")) {
            UNIT_FAIL("Expected inline, disasm:\n" + str.Str());
        }
#endif
    }

    Y_UNIT_TEST(UseExternalFromGeneratedFunction_128bit_Bitcode) {
        auto codegen = ICodegen::Make(ETarget::Native);
        auto bitcode = NResource::Find("/llvm_bc/Funcs");
        codegen->LoadBitCode(bitcode, "Funcs");
        auto func = CreateUseExternalFromGeneratedFunction128(codegen, true);
        codegen->ExportSymbol(func);
        codegen->Verify();
        codegen->Compile();
        TStringStream str;
        codegen->ShowGeneratedFunctions(&str);
#ifdef _win_
        typedef T128(*TFunc)(T128, T128, T128);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT(funcPtr(T128(7), T128(4), T128(8)) == T128(4289));
#else
        typedef unsigned __int128(*TFunc)(__int128, __int128, __int128);
        auto funcPtr = (TFunc)codegen->GetPointerToFunction(func);
        UNIT_ASSERT(funcPtr(7, 4, 8) == 4289);
#endif
#if !defined(_asan_enabled_) && !defined(_msan_enabled_) && !defined(_tsan_enabled_)
        if (str.Str().Contains("call")) {
            UNIT_FAIL("Expected inline, disasm:\n" + str.Str());
        }
#endif
    }
}

#endif
