#include "dq_factories.h"

#include <ydb/library/yql/dq/comp_nodes/dq_program_builder.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>


namespace NKikimr::NMiniKQL {

template<bool LLVM, bool Spilling = false>
struct TDqSetup: public TSetup<LLVM, Spilling> {
    explicit TDqSetup(TComputationNodeFactory nodeFactory = GetDqNodeFactory(), TVector<TUdfModuleInfo>&& modules = {})
        : TSetup<LLVM, Spilling>(nodeFactory, std::move(modules)) {
        this->PgmBuilder = MakeHolder<TDqProgramBuilder>(*this->Env, *this->FunctionRegistry);
    }

    TDqProgramBuilder& GetDqProgramBuilder() {
        return static_cast<TDqProgramBuilder&>(*this->PgmBuilder);
    }
};

} // namespace NKikimr::NMiniKQL
