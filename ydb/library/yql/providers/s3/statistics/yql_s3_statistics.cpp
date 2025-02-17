#include <ydb/library/yql/providers/s3/expr_nodes/yql_s3_expr_nodes.h>

#include "yql_s3_statistics.h"


namespace NYql {

double GetDqSourceWrapBaseCost(const NNodes::TDqSourceWrapBase& wrapBase, TTypeAnnotationContext& typeCtx) {
    if (auto maybeS3DataSource = wrapBase.DataSource().Maybe<NNodes::TS3DataSource>()) {
        auto s3DataSource = maybeS3DataSource.Cast();
        auto stats = typeCtx.GetStats(s3DataSource.Raw());
        if (stats && stats->Specific) {
            const TS3ProviderStatistics* specific = dynamic_cast<const TS3ProviderStatistics*>((stats->Specific.get()));
            auto rowType = wrapBase.RowType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>();
            auto it = specific->Costs.find(TStructExprType::MakeHash(rowType->GetItems()));
            if (it != specific->Costs.end()) {
                return it->second;
            }
        }
    }
    return 0.0;
}

} // namespace NYql
