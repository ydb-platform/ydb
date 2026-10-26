#include "kqp_olap_aggregate.h"
#include "kqp_rbo.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

// Column shards compute AVG as SUM and COUNT, which build its intermediate state.
const THashSet<TString> OlapAggregateFunctions{"count", "sum", "min", "max", "avg", "some"};

const TDataExprType* GetDataType(const TTypeAnnotationNode* type) {
    if (!type) {
        return nullptr;
    }
    const auto* itemType = &RemoveOptionality(*type);
    return itemType->GetKind() == ETypeAnnotationKind::Data ? itemType->Cast<TDataExprType>() : nullptr;
}

// Whether column shards compute `aggFunction` over a column of `type` the way compute does.
bool IsOlapAggregateType(const TString& aggFunction, const TTypeAnnotationNode* type) {
    const auto* dataType = GetDataType(type);
    // Column shards do not aggregate decimals.
    if (!dataType || IsDataTypeDecimal(dataType->GetSlot())) {
        return false;
    }
    const auto slot = dataType->GetSlot();
    // Storage widens Float sums to Double, whereas compute accumulates in Float.
    // Keep SUM(Float) in compute to preserve both its type and its rounding.
    if (aggFunction == "sum" && slot == EDataSlot::Float) {
        return false;
    }
    // Compute orders NaN above every other value, whereas column shards compare
    // with `<` and may drop it. Keep MIN and MAX of floating-point values in compute.
    if ((aggFunction == "min" || aggFunction == "max") && IsDataTypeFloat(slot)) {
        return false;
    }
    if (aggFunction == "sum" || aggFunction == "avg") {
        return IsDataTypeNumeric(slot);
    }
    return true;
}

// Projections overwrite read columns, while column shards aggregate the stored
// values. A read program also aggregates or deduplicates at most once.
bool HasOlapProgramBlockingAggregate(const TOpRead& read) {
    if (!read.OlapFilterLambda) {
        return false;
    }
    return !!FindNode(read.OlapFilterLambda, [](const TExprNode::TPtr& node) {
        return TKqpOlapProjections::Match(node.Get()) || TKqpOlapAgg::Match(node.Get()) || TKqpOlapDistinct::Match(node.Get());
    });
}

// The read column `input` refers to, directly or through its copy in the Map.
std::optional<TInfoUnitId> ResolveReadColumn(TInfoUnitId input, const TOpRead& read, const TOpMap* map) {
    if (read.GetColumns().Contains(input)) {
        return input;
    }
    const auto* element = map ? map->FindOutputElement(input) : nullptr;
    if (element && element->IsColumnAccess() && read.GetColumns().Contains(element->GetColumnAccess())) {
        return element->GetColumnAccess();
    }
    return std::nullopt;
}

// COUNT(*) counts Void or another literal of the Map, which is never null.
bool IsCountAll(const TOpAggregationTraits& traits, const TOpMap* map) {
    const auto* element = map ? map->FindOutputElement(traits.Input) : nullptr;
    if (traits.AggFunction != "count" || !element || element->IsColumnAccess()) {
        return false;
    }
    const auto body = element->GetExpression().GetExpressionBody();
    return TCoVoid::Match(body.Get()) || TCoDataCtor::Match(body.Get());
}

// The result column of a trivial DISTINCT holding `key`. DISTINCT names it after
// its select item, which a projection pushed into the read may no longer name.
TInfoUnitId GetDistinctKeyOutput(const TOpAggregate& aggregate, TInfoUnitId key) {
    if (!aggregate.IsDistinctAll()) {
        return key;
    }
    auto output = key;
    for (const auto& [id, traits] : aggregate.GetAggregationTraits().Items()) {
        if (traits.Input == key) {
            output = id;
            break;
        }
    }
    // A split DISTINCT outputs the result columns from its final aggregate.
    if (aggregate.GetAggregationPhase() != EOpPhase::Intermediate || aggregate.Parents.size() != 1
        || aggregate.Parents.front().first->Kind != EOperator::Aggregate) {
        return output;
    }
    const auto& finalAggregate = CastOperator<TOpAggregate>(*aggregate.Parents.front().first);
    if (!finalAggregate.IsDistinctAll()) {
        return output;
    }
    for (const auto& [id, traits] : finalAggregate.GetAggregationTraits().Items()) {
        if (traits.Input == output) {
            return id;
        }
    }
    return output;
}

} // anonymous namespace

bool IsOlapAverageCast(const TTypeAnnotationNode& columnType) {
    return RemoveOptionality(columnType).Cast<TDataExprType>()->GetSlot() != EDataSlot::Double;
}

bool HasOlapAverageCast(const TOlapAggregate& aggregate, TExprContext& ctx) {
    for (const auto& aggregation : aggregate.Aggregations) {
        if (aggregation.Function == "avg" && IsOlapAverageCast(*aggregate.Read->GetIUType(*aggregation.Column, ctx))) {
            return true;
        }
    }
    return false;
}

std::optional<TOlapAggregate> MatchOlapAggregate(const TOpAggregate& aggregate, TExprContext& ctx) {
    const auto& aggregations = aggregate.GetAggregationTraits();
    if (aggregate.IsDistinctAll() || aggregations.Keys().Empty()) {
        return std::nullopt;
    }

    TOlapAggregate result;
    auto* input = aggregate.GetInput().Get();
    if (input->Kind == EOperator::Map) {
        result.Map = CastOperator<TOpMap>(input);
        input = result.Map->GetInput().Get();
    }
    if (input->Kind != EOperator::Source) {
        return std::nullopt;
    }
    result.Read = CastOperator<TOpRead>(input);

    const auto& read = *result.Read;
    // The read program computes the aggregate, so all of them share a stage.
    const auto stageId = aggregate.Props.StageId;
    if (!stageId || read.Props.StageId != stageId || (result.Map && result.Map->Props.StageId != stageId)) {
        return std::nullopt;
    }
    if (read.GetTableStorageType() != NYql::EStorageType::ColumnStorage || read.Limit || read.SortDir != ESortDir::None
        || HasOlapProgramBlockingAggregate(read)) {
        return std::nullopt;
    }

    // Maps only append definitions, so read keys reach the aggregate as is.
    TUnorderedIUs keys;
    for (const auto key : aggregate.GetKeyColumns().Items()) {
        if (!read.GetColumns().Contains(key) || !GetDataType(read.GetIUType(key, ctx))) {
            return std::nullopt;
        }
        if (keys.Add(key)) {
            result.Keys.push_back(key);
        }
    }

    for (const auto& [output, traits] : aggregations.Items()) {
        if (traits.Distinct || traits.Unwrap || !OlapAggregateFunctions.contains(traits.AggFunction)) {
            return std::nullopt;
        }
        if (const auto column = ResolveReadColumn(traits.Input, read, result.Map)) {
            if (!IsOlapAggregateType(traits.AggFunction, read.GetIUType(*column, ctx))) {
                return std::nullopt;
            }
            result.Aggregations.push_back({output, traits.AggFunction, *column});
        } else if (IsCountAll(traits, result.Map)) {
            result.Aggregations.push_back({output, traits.AggFunction, std::nullopt});
        } else {
            return std::nullopt;
        }
    }

    return result;
}

std::optional<TOlapDistinct> MatchOlapDistinct(const TOpAggregate& aggregate, const TString& forcedKey,
                                               const TInfoUnitRegistry& registry) {
    const auto& keys = aggregate.GetKeyColumns().Unordered();
    if (!aggregate.IsTrivialDistinct() || keys.Size() != 1) {
        return std::nullopt;
    }
    const IOperator* consumer = &aggregate;
    const TOpMap* map = nullptr;
    auto* input = aggregate.GetInput().Get();
    if (input->Kind == EOperator::Map) {
        map = CastOperator<TOpMap>(input);
        consumer = map;
        input = map->GetInput().Get();
    }
    if (input->Kind != EOperator::Source) {
        return std::nullopt;
    }
    auto* read = CastOperator<TOpRead>(input);
    if (read->GetTableStorageType() != NYql::EStorageType::ColumnStorage) {
        return std::nullopt;
    }

    const auto key = *keys.begin();
    const auto keyName = registry.Get(GetDistinctKeyOutput(aggregate, key)).GetColumnName();
    Y_ENSURE(keyName == forcedKey,
             "OptForceOlapPushdownDistinct = '" << forcedKey << "' does not match DISTINCT key column '" << keyName << "'");
    // Column shards mark a stored or projected column, not a key computed in compute.
    const auto column = ResolveReadColumn(key, *read, map);
    if (!column || read->Limit || read->SortDir != ESortDir::None || read->Parents.size() != 1
        || (map && map->Parents.size() != 1)) {
        return std::nullopt;
    }
    // The program outputs only the key column, so the consumer may use nothing else.
    const auto keyColumn = registry.Get(*column).GetColumnName();
    for (const auto id : GetLiveIn(consumer, 0)) {
        if (registry.Get(id).GetColumnName() != keyColumn) {
            return std::nullopt;
        }
    }
    return TOlapDistinct{read, *column};
}

} // namespace NKikimr::NKqp
