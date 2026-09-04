#include "yql_pq_factory.h"
#include "dq_pq_parsing_wrapper.h"

namespace NYql {

using namespace NKikimr::NMiniKQL;

TComputationNodeFactory GetDqPqFactory() {
    return [] (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (const auto name = callable.GetType()->GetName(); name == "DqPqParsingWrapper") {
            return WrapDqPqParsingWrapper(callable, ctx);
        }

        return nullptr;
    };
}

} // namespace NYql
