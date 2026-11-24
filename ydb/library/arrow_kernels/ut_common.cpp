#include "func_common.h"
#include "functions.h"
#include "ut_common.h"

#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/registry.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/registry_internal.h>
#include <util/system/yassert.h>

#include <memory>

namespace cp = ::arrow::compute;

namespace NKikimr::NKernels {

static void RegisterMath(cp::FunctionRegistry* registry) {
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TAcosh>(TAcosh::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TAtanh>(TAtanh::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TCbrt>(TCbrt::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TCosh>(TCosh::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeConstNullary<TE>(TE::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TErf>(TErf::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TErfc>(TErfc::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TExp>(TExp::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TExp2>(TExp2::Name)).ok());
    // Temporarily disabled because of compilation error on Windows.
#if 0
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TExp10>(TExp10::Name)).ok());
#endif
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathBinary<THypot>(THypot::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TLgamma>(TLgamma::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeConstNullary<TPi>(TPi::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TSinh>(TSinh::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TSqrt>(TSqrt::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeMathUnary<TTgamma>(TTgamma::Name)).ok());
}

static void RegisterRound(cp::FunctionRegistry* registry) {
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticUnary<TRound>(TRound::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticUnary<TRoundBankers>(TRoundBankers::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticUnary<TRoundToExp2>(TRoundToExp2::Name)).ok());
}

static void RegisterArithmetic(cp::FunctionRegistry* registry) {
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticIntBinary<TGreatestCommonDivisor>(TGreatestCommonDivisor::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticIntBinary<TLeastCommonMultiple>(TLeastCommonMultiple::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticBinary<TModulo>(TModulo::Name)).ok());
    Y_ABORT_UNLESS(registry->AddFunction(MakeArithmeticBinary<TModuloOrZero>(TModuloOrZero::Name)).ok());
}

static std::unique_ptr<cp::FunctionRegistry> CreateCustomRegistry() {
    auto registry = cp::FunctionRegistry::Make();
    RegisterMath(registry.get());
    RegisterRound(registry.get());
    RegisterArithmetic(registry.get());
    cp::internal::RegisterScalarCast(registry.get());
    return registry;
}

// Creates singleton custom registry
cp::FunctionRegistry* GetCustomFunctionRegistry() {
    static auto g_registry = CreateCustomRegistry();
    return g_registry.get();
}

// We want to have ExecContext per thread. All these context use one custom registry.
cp::ExecContext* GetCustomExecContext() {
    static thread_local cp::ExecContext context(arrow::default_memory_pool(), nullptr, GetCustomFunctionRegistry());
    return &context;
}

}   // namespace NKikimr::NKernels
