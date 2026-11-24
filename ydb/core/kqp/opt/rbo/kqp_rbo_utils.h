#pragma once

#include "kqp_info_unit.h"
#include "kqp_rbo_context.h"
#include "kqp_plan_props.h"

#include <algorithm>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

class IOperator;
class TOpAggregate;
class TExpression;

bool ReferencesUnresolvedSubplan(const TExpression& expr, const TPlanProps& props);
// Ordered keys for a new aggregation shuffle, expressed in input/output column IDs.
// Positions correspond; preserved input partitioning may use a different order or subset.
TOrderedIUs<> GetAggregateInputShuffle(const TOpAggregate& aggregate);
TOrderedIUs<> GetAggregateOutputShuffle(const TOpAggregate& aggregate);
TOrderedIUs<> GetAggregatePreservedShuffling(const TOpAggregate& aggregate, const TRBOContext& ctx);
bool CanEliminateAggregateShuffle(const TOpAggregate& aggregate, const TRBOContext& ctx);

bool JoinOutputsLeft(const TString& joinKind);
bool JoinOutputsRight(const TString& joinKind);
TString GetValidJoinKind(const TString& joinKind);

bool SortMatchesKeyOrder(const TVector<TString>& sortColumns, const TVector<TString>& keyColumns, size_t pointPrefixLen);
// Whether a sort orders all its keys in one direction, as a datashard read in the key order (true) or in the reverse
// order (false) returns rows; nothing if the sort has no keys or mixes the directions.
std::optional<bool> GetKeyReadDirection(const TSortIUs& sortElements);

}
}
