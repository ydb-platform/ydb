#include "yql_ydb_factory.h"
#include "yql_kik_scan.h"

namespace NYql {

using namespace NKikimr::NMiniKQL;

TComputationNodeFactory GetDqYdbFactory(NYdb::TDriver driver) {
    return [driver] (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (const auto& name = callable.GetType()->GetName(); name == "KikScan") {
                return NDqs::WrapKikScan<false>(callable, ctx, driver);
            } else if (name == "KikScanAsync") {
                return NDqs::WrapKikScan<true>(callable, ctx, driver);
            }

            return nullptr;
        };
}

} // NYql
