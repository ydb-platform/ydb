#include "functions.h"
#include "func_common.h"
#include <util/system/yassert.h> 
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/cast.cc> 
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h> 

namespace cp = ::arrow::compute;

namespace NKikimr::NArrow {

static void RegisterMath(cp::FunctionRegistry* registry) {
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TAcosh>(TAcosh::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TAtanh>(TAtanh::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TCbrt>(TCbrt::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TCosh>(TCosh::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeConstNullary<TE>(TE::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TErf>(TErf::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TErfc>(TErfc::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TExp>(TExp::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TExp2>(TExp2::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TExp10>(TExp10::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathBinary<THypot>(THypot::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TLgamma>(TLgamma::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeConstNullary<TPi>(TPi::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TSinh>(TSinh::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TSqrt>(TSqrt::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeMathUnary<TTgamma>(TTgamma::Name)).ok());
}

static void RegisterRound(cp::FunctionRegistry* registry) {
    Y_VERIFY(registry->AddFunction(MakeArithmeticUnary<TRound>(TRound::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeArithmeticUnary<TRoundBankers>(TRoundBankers::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeArithmeticUnary<TRoundToExp2>(TRoundToExp2::Name)).ok());
}

static void RegisterArithmetic(cp::FunctionRegistry* registry) {
    Y_VERIFY(registry->AddFunction(MakeArithmeticIntBinary<TGreatestCommonDivisor>(TGreatestCommonDivisor::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeArithmeticIntBinary<TLeastCommonMultiple>(TLeastCommonMultiple::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeArithmeticBinary<TModulo>(TModulo::Name)).ok());
    Y_VERIFY(registry->AddFunction(MakeArithmeticBinary<TModuloOrZero>(TModuloOrZero::Name)).ok());
}


static std::unique_ptr<cp::FunctionRegistry> CreateCustomRegistry() {
    auto registry = cp::FunctionRegistry::Make();
    RegisterMath(registry.get());
    RegisterRound(registry.get());
    RegisterArithmetic(registry.get());
    cp::internal::RegisterScalarCast(registry.get());
    return registry;
}

cp::FunctionRegistry* GetCustomFunctionRegistry() {
    static auto g_registry = CreateCustomRegistry();
    return g_registry.get();
}

cp::ExecContext* GetCustomExecContext() {
    static auto context = std::make_unique<cp::ExecContext>(arrow::default_memory_pool(), NULLPTR, GetCustomFunctionRegistry());
    return context.get();
}

}
