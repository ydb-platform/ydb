#include "factories.h"

#include <ydb/core/kqp/runtime/kqp_program_builder.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>


namespace NKikimr {
namespace NMiniKQL {

template<bool LLVM, bool Spilling = false>
struct TKqpSetup: public TSetup<LLVM, Spilling>
{
    explicit TKqpSetup(TComputationNodeFactory nodeFactory = GetPerfTestFactory(), TVector<TUdfModuleInfo>&& modules = {})
        : TSetup<LLVM, Spilling>(nodeFactory, std::move(modules))
    {
        this->PgmBuilder = MakeHolder<TKqpProgramBuilder>(*this->Env, *this->FunctionRegistry);
    }

    TKqpProgramBuilder& GetKqpBuilder()
    {
        return static_cast<TKqpProgramBuilder&>(*this->PgmBuilder);
    }
};

}
}