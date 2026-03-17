#include "kqp_statistics.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

bool TKqpStatsStore::ContainsStats(const NYql::TExprNode* input) const {
    return Map_.contains(input->UniqueId());
}

std::shared_ptr<TOptimizerStatistics> TKqpStatsStore::GetStats(const NYql::TExprNode* input) const {
    auto it = Map_.find(input->UniqueId());
    if (it == Map_.end()) {
        return nullptr;
    }
    return it->second;
}

void TKqpStatsStore::SetStats(const NYql::TExprNode* input, std::shared_ptr<TOptimizerStatistics> stats) {
    Map_[input->UniqueId()] = std::move(stats);
}

} // namespace NKikimr::NKqp
