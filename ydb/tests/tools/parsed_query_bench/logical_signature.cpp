#include <ydb/core/kqp/opt/rbo/kqp_operator.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp::NParsedQueryBench {
namespace {

// Prefix lengths distinguish names and arbitrary binary scalar literals.
TString Tuple(TStringBuf kind, const TVector<TString>& parts) {
    TStringBuilder out;
    out << kind << "(";
    for (const auto& part : parts) {
        Y_ENSURE(out.size() + part.size() <= (16 << 20), "Logical comparison size limit exceeded");
        out << part.size() << ":" << part;
    }
    return out << ")";
}

class TSignature {
public:
    TString Read(TOpRoot& root) {
        const auto input = Relation(*root.GetInput(), root.PlanProps.InfoUnitRegistry);
        TVector<TString> result{input};
        for (const auto& [id, label] : root.GetColumns().Items()) {
            result.push_back(Tuple("output", {label, Values.at(id)}));
        }
        return Tuple("root", result);
    }

private:
    TString Expr(const TExprNode& node) {
        if (node.IsCallable("Member")) {
            return Values.at(GetMemberId(node));
        }
        if (const auto* previous = Expressions.FindPtr(&node)) {
            return *previous;
        }
        Y_ENSURE(node.IsCallable() || node.IsAtom() || node.IsList(), "Unsupported scalar comparison node");
        TVector<TString> parts;
        if (!node.IsList()) {
            parts.emplace_back(node.Content());
        }
        for (const auto& child : node.Children()) {
            parts.push_back(Expr(*child));
        }
        const auto signature = Tuple(node.IsCallable() ? "call" : node.IsAtom() ? "atom" : "list", parts);
        Expressions.emplace(&node, signature);
        return signature;
    }

    TString Relation(IOperator& op, const TInfoUnitRegistry& registry) {
        TVector<TString> parts;
        for (auto* child : op.GetChildren()) {
            parts.push_back(Relation(*child, registry));
        }
        switch (op.Kind) {
            case EOperator::EmptySource: {
                Y_ENSURE(!CastOperator<TOpEmptySource>(op).Input, "Unsupported non-unit row source");
                return Tuple("unit", {});
            }
            case EOperator::Source: {
                const auto& read = CastOperator<TOpRead>(op);
                Y_ENSURE(!read.RangeInfo && !read.OlapFilterLambda && !read.Limit && !read.OriginalPredicate
                    && !read.RequireSortedRead && read.SortDir == ESortDir::None, "Unsupported scan restriction");
                const auto scan = Tuple("read", {ToString(++Scan), TString(read.GetTable()->Head().Content())});
                for (const auto id : read.GetColumns()) {
                    Values[id] = Tuple("column", {scan, registry.Get(id).GetColumnName()});
                }
                return scan;
            }
            case EOperator::Replicate: {
                const auto& port = CastOperator<TOpReplicate>(op);
                Y_ENSURE(port.IsPrimary() && port.GetReplicate().GetOutputs().size() == 1,
                    "Unsupported shared plan in logical comparison");
                return parts.front();
            }
            case EOperator::Map: {
                for (const auto& [id, item] : CastOperator<TOpMap>(op).GetMapElements().Items()) {
                    Values[id] = Expr(*item.GetExpression().GetExpressionBody());
                }
                return parts.front();
            }
            case EOperator::Filter: {
                parts.push_back(Expr(*CastOperator<TOpFilter>(op).GetFilterExpression().GetExpressionBody()));
                return Tuple("filter", parts);
            }
            case EOperator::Join: {
                const auto& join = CastOperator<TOpJoin>(op);
                parts.push_back(join.JoinKind);
                for (const auto& key : join.JoinKeys.Items()) {
                    parts.push_back(Tuple(key.EqualNulls ? "null-safe" : "equal", {Values.at(key.first), Values.at(key.second)}));
                }
                for (const auto& filter : join.JoinFilters) {
                    parts.push_back(Expr(*filter.GetExpressionBody()));
                }
                return Tuple("join", parts);
            }
            case EOperator::Aggregate: {
                const auto& aggregate = CastOperator<TOpAggregate>(op);
                Y_ENSURE(aggregate.GetKeyColumns().Items().empty() && !aggregate.IsDistinctAll(),
                    "Unsupported grouping in logical comparison");
                for (const auto& [id, item] : aggregate.GetAggregationTraits().Items()) {
                    Y_ENSURE(item.AggFunction == "count" && !item.Unwrap, "Unsupported aggregate comparison");
                    auto argument = Values.at(item.Input);
                    // Legacy represents COUNT(*) with Void(), NewParser uses a non-null literal.
                    if (!item.Distinct && (argument == Tuple("call", {"Void"})
                        || argument == Tuple("call", {"Int32", Tuple("atom", {"1"})})
                        || argument == Tuple("call", {"Uint64", Tuple("atom", {"1"})}))) {
                        argument = "all rows";
                    }
                    Values[id] = Tuple(item.Distinct ? "count distinct" : "count", {argument});
                }
                return Tuple("aggregate", parts);
            }
            default:
                ythrow yexception() << "Unsupported operator in logical comparison: " << op.GetExplainName();
        }
    }

    size_t Scan = 0;
    THashMap<TInfoUnitId, TString> Values;
    THashMap<const TExprNode*, TString> Expressions;
};

} // namespace

TString LogicalSignature(TOpRoot& root) {
    return TSignature().Read(root);
}

} // namespace NKikimr::NKqp::NParsedQueryBench
