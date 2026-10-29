#include "kqp_read_ranges_structure.h"

#include "kqp_olap_expr_inspection.h"
#include "kqp_read_ranges_model.h"
#include "kqp_read_ranges_sql_in.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>

namespace NKikimr::NKqp::NOpt::NReadRanges {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

TString DescribeExprValue(const TExprNode::TPtr& node) {
    if (const auto param = TMaybeNode<TCoParameter>(node)) {
        return TString(param.Cast().Name().Value());
    }
    if (const auto value = ParseKeyValue(node)) {
        return FormatKeyValue(*value);
    }
    if (node->IsCallable("Just") && node->ChildrenSize() == 1) {
        return DescribeExprValue(node->HeadPtr());
    }
    if (node->IsCallable("Map") && node->ChildrenSize() == 2 && node->Tail().IsLambda()
        && node->Tail().Head().ChildrenSize() == 1
        && node->Tail().TailPtr() == node->Tail().Head().HeadPtr()) {
        // The extractor wraps an optional list in an identity Map.
        return DescribeExprValue(node->HeadPtr());
    }
    if (node->IsCallable({"SafeCast", "StrictCast", "Convert", "ToString"}) && node->ChildrenSize() > 0) {
        return TStringBuilder() << node->Content() << '(' << DescribeExprValue(node->HeadPtr()) << ')';
    }
    return TStringBuilder() << (node->IsCallable() ? node->Content() : "expr") << "(…)";
}

// The values one key column takes: a set of points, an interval, or another set described as text.
struct TColumnSet {
    enum class EKind {
        Points,
        Interval,
        Other,
    };

    EKind Kind = EKind::Other;
    TVector<TString> Points;
    std::optional<TString> Lower; // -∞ when not set
    bool LowerIncluded = false;
    std::optional<TString> Upper; // +∞ when not set
    bool UpperIncluded = false;
    TString Text;
    bool Compound = false; // Text is a union or an intersection of sets

    TString Format() const {
        TStringBuilder result;
        if (Kind == EKind::Points) {
            result << '{';
            for (size_t i = 0; i < Min(Points.size(), MaxDescribedPoints); ++i) {
                result << (i ? ", " : "") << Points[i];
            }
            if (Points.size() > MaxDescribedPoints) {
                result << ", … " << Points.size() - MaxDescribedPoints << " more";
            }
            result << '}';
        } else if (Kind == EKind::Interval) {
            if (Lower) {
                result << (LowerIncluded ? '[' : '(') << *Lower;
            } else {
                result << "(-∞";
            }
            result << ", ";
            if (Upper) {
                result << *Upper << (UpperIncluded ? ']' : ')');
            } else {
                result << "+∞)";
            }
        } else {
            result << Text;
        }
        return result;
    }

    TString FormatOperand() const {
        return Compound ? TString(TStringBuilder() << '(' << Format() << ')') : Format();
    }
};

std::optional<TColumnSet> DescribeRangeFor(const TExprNode& node) {
    using EKind = TColumnSet::EKind;
    const auto op = node.Head().Content();
    const auto value = DescribeExprValue(node.ChildPtr(1));
    if (op == "==" || op == "===") {
        return TColumnSet{.Kind = EKind::Points, .Points = {value}};
    }
    if (op == "<" || op == "<=") {
        return TColumnSet{.Kind = EKind::Interval, .Upper = value, .UpperIncluded = op == "<="};
    }
    if (op == ">" || op == ">=") {
        return TColumnSet{.Kind = EKind::Interval, .Lower = value, .LowerIncluded = op == ">="};
    }
    if (op == "!=") {
        return TColumnSet{.Text = TStringBuilder() << "(-∞, " << value << ") ∪ (" << value << ", +∞)", .Compound = true};
    }
    if (op == "Exists") {
        // NULL sorts before every other value.
        return TColumnSet{.Kind = EKind::Interval, .Lower = "NULL"};
    }
    if (op == "NotExists") {
        return TColumnSet{.Kind = EKind::Points, .Points = {"NULL"}};
    }
    if (op == "StartsWith" || op == "NotStartsWith") {
        return TColumnSet{.Text = TStringBuilder() << (op == "NotStartsWith" ? "NOT " : "") << "StartsWith(" << value << ')'};
    }
    return std::nullopt;
}

// The values of IN: single values, or tuples in key order.
TColumnSet DescribeSqlIn(const TSqlInRanges& in) {
    if (!in.Items) {
        // A list known only at runtime, usually a parameter.
        return TColumnSet{.Text = DescribeExprValue(in.List)};
    }
    // Describe the input set. Its length alone cannot establish a range-limit overflow: NULLs and values
    // that do not fit the key type may produce no ranges. Only MiniKQL determines the actual scan bounds.
    TColumnSet result{.Kind = TColumnSet::EKind::Points};
    THashSet<TString> seen;
    for (const auto& item : *in.Items) {
        TStringBuilder point;
        for (size_t i = 0; i < in.Columns.size(); ++i) {
            const auto value = in.GetValue(item, in.Columns[i]);
            point << (i ? ", " : "") << (value ? DescribeExprValue(value) : TString("…"));
        }
        const TString text = in.Columns.size() > 1 ? TString(TStringBuilder() << '(' << point << ')') : TString(point);
        if (seen.insert(text).second) {
            result.Points.push_back(text);
        }
    }
    return result;
}

TColumnSet JoinSets(const TVector<TColumnSet>& sets, TStringBuf op) {
    TColumnSet result{.Compound = true};
    TStringBuilder text;
    for (size_t i = 0; i < sets.size(); ++i) {
        text << (i ? op : "") << sets[i].FormatOperand();
    }
    result.Text = text;
    return result;
}

TColumnSet UniteSets(const TVector<TColumnSet>& sets) {
    if (AllOf(sets, [](const TColumnSet& set) { return set.Kind == TColumnSet::EKind::Points; })) {
        TColumnSet result{.Kind = TColumnSet::EKind::Points};
        THashSet<TString> seen;
        for (const auto& set : sets) {
            for (const auto& point : set.Points) {
                if (seen.insert(point).second) {
                    result.Points.push_back(point);
                }
            }
        }
        return result;
    }
    return JoinSets(sets, " ∪ ");
}

TColumnSet IntersectSets(const TVector<TColumnSet>& sets) {
    // Half-bounded intervals with one lower and one upper bound are one interval.
    TColumnSet interval{.Kind = TColumnSet::EKind::Interval};
    for (const auto& set : sets) {
        if (set.Kind != TColumnSet::EKind::Interval || (set.Lower && interval.Lower) || (set.Upper && interval.Upper)) {
            return JoinSets(sets, " ∩ ");
        }
        if (set.Lower) {
            interval.Lower = set.Lower;
            interval.LowerIncluded = set.LowerIncluded;
        }
        if (set.Upper) {
            interval.Upper = set.Upper;
            interval.UpperIncluded = set.UpperIncluded;
        }
    }
    return interval;
}

// Describes a ranges expression without computing it. Every node covers some key columns starting from a given one:
// RangeFor covers one, RangeMultiply puts its children one after another, RangeUnion and RangeIntersect combine
// children over the same columns. The RangeMultiply limit is not applied, so past it the read is coarser than shown.
class TRangesDescriber {
public:
    explicit TRangesDescriber(TConstArrayRef<TString> keyColumns)
        : KeyColumns_(keyColumns)
    {}

    std::optional<TString> Describe(const TExprNode::TPtr& node) {
        const auto description = DescribeNode(node, 0);
        if (!description) {
            return std::nullopt;
        }
        return Render(*description, 0, EOp::None);
    }

private:
    enum class EOp {
        None,
        Union,
        Intersect,
        Product,
        Condition,
    };

    struct TDescription {
        size_t Width = 0;
        std::optional<TColumnSet> Column; // the description of a single column
        TString Text;
        EOp Op = EOp::None;
        bool Unconstrained = false; // the full range, which a product leaves out
    };

    TString ColumnName(size_t column) const {
        return NOpt::GetOlapColumnName(KeyColumns_[column], true);
    }

    TString Render(const TDescription& description, size_t column, EOp parent) const {
        if (description.Column) {
            const TString text = TStringBuilder() << ColumnName(column) << " ∈ " << description.Column->Format();
            return description.Column->Compound && parent != EOp::None ? TString(TStringBuilder() << '(' << text << ')') : text;
        }
        if (description.Op != EOp::None && parent != EOp::None && description.Op != parent) {
            return TStringBuilder() << '(' << description.Text << ')';
        }
        return description.Text;
    }

    std::optional<TDescription> DescribeNode(const TExprNode::TPtr& node, size_t column) {
        if (const auto in = MatchSqlInRanges(node)) {
            const size_t width = in->Columns.size();
            if (column + width > KeyColumns_.size()) {
                return std::nullopt;
            }
            auto set = DescribeSqlIn(*in);
            if (!in->Items && in->Columns.front().Element) {
                // The collection still contains tuples in their original order. Put the column names in that
                // order too; a projection or duplicate component cannot be expressed as tuple membership.
                if (!in->TupleSize || *in->TupleSize != width) {
                    return std::nullopt;
                }
                TVector<TString> names(width);
                for (size_t i = 0; i < width; ++i) {
                    const auto element = in->Columns[i].Element;
                    if (!element || *element >= in->KeyOrder.size()) {
                        return std::nullopt;
                    }
                    const auto source = in->KeyOrder[*element];
                    if (source >= width || !names[source].empty()) {
                        return std::nullopt;
                    }
                    names[source] = ColumnName(column + i);
                }
                TStringBuilder text;
                text << '(';
                for (size_t i = 0; i < width; ++i) {
                    text << (i ? ", " : "") << names[i];
                }
                text << ") ∈ " << set.Format();
                return TDescription{.Width = width, .Text = text};
            }
            if (width == 1) {
                return TDescription{.Width = 1, .Column = std::move(set)};
            }
            TStringBuilder text;
            text << '(';
            for (size_t i = 0; i < width; ++i) {
                text << (i ? ", " : "") << ColumnName(column + i);
            }
            text << ") ∈ " << set.Format();
            return TDescription{.Width = width, .Text = text};
        }
        if (node->IsCallable({"RangeFinalize", "RangeToPg"}) && node->ChildrenSize() == 1) {
            return DescribeNode(node->HeadPtr(), column);
        }
        if (node->IsCallable("If") && node->ChildrenSize() == 3) {
            // A condition that does not depend on the row: the ranges if it holds, nothing otherwise.
            const auto condition = ParseKeyValue(node->HeadPtr());
            if (condition && condition->Kind == TKeyValue::EKind::Data && condition->Slot == NUdf::EDataSlot::Bool) {
                return DescribeNode(condition->Value.Get<bool>() ? node->ChildPtr(1) : node->ChildPtr(2), column);
            }
            if (!node->Child(2)->IsCallable("RangeEmpty")) {
                return std::nullopt;
            }
            auto ranges = DescribeNode(node->ChildPtr(1), column);
            if (!ranges) {
                return std::nullopt;
            }
            return TDescription{
                .Width = ranges->Width,
                .Text = TStringBuilder() << Render(*ranges, column, EOp::Condition) << " if " << DescribeExprValue(node->HeadPtr()),
                .Op = EOp::Condition,
            };
        }
        if (node->IsCallable("RangeFor") && node->ChildrenSize() == 3) {
            if (column >= KeyColumns_.size()) {
                return std::nullopt;
            }
            auto set = DescribeRangeFor(*node);
            if (!set) {
                return std::nullopt;
            }
            return TDescription{.Width = 1, .Column = std::move(*set)};
        }
        if (node->IsCallable("RangeEmpty")) {
            return TDescription{.Width = ParseRangeEmpty(*node).Width, .Text = "∅"};
        }
        if (node->IsCallable("AsRange")) {
            const auto ranges = ParseAsRange(*node);
            if (!ranges || column + ranges->Width > KeyColumns_.size()) {
                return std::nullopt;
            }
            const auto columns = KeyColumns_.subspan(column, ranges->Width);
            TStringBuilder text;
            for (size_t i = 0; i < ranges->Ranges.size(); ++i) {
                text << (i ? " ∪ " : "") << FormatRange(ranges->Ranges[i], columns);
            }
            const bool unconstrained = ranges->Ranges.size() == 1 && AllOf(ranges->Ranges.front().Left.Components, [](const TComponent& component) {
                return component.Inf != 0;
            }) && AllOf(ranges->Ranges.front().Right.Components, [](const TComponent& component) { return component.Inf != 0; });
            if (unconstrained && ranges->Width == 1) {
                return TDescription{.Width = 1, .Column = TColumnSet{.Kind = TColumnSet::EKind::Interval}, .Unconstrained = true};
            }
            return TDescription{
                .Width = ranges->Width,
                .Text = ranges->Ranges.empty() ? TString("∅") : TString(text),
                .Op = ranges->Ranges.size() > 1 ? EOp::Union : EOp::None,
                .Unconstrained = unconstrained,
            };
        }
        if (node->IsCallable({"RangeUnion", "RangeIntersect"}) && node->ChildrenSize() > 0) {
            const auto op = node->IsCallable("RangeUnion") ? EOp::Union : EOp::Intersect;
            TVector<TDescription> children;
            for (const auto& child : node->Children()) {
                auto description = DescribeNode(child, column);
                if (!description || (!children.empty() && description->Width != children.front().Width)) {
                    return std::nullopt;
                }
                children.push_back(std::move(*description));
            }
            if (children.size() == 1) {
                return std::move(children.front());
            }
            if (AllOf(children, [](const TDescription& child) { return child.Column.has_value(); })) {
                TVector<TColumnSet> sets;
                for (auto& child : children) {
                    sets.push_back(std::move(*child.Column));
                }
                return TDescription{.Width = children.front().Width, .Column = op == EOp::Union ? UniteSets(sets) : IntersectSets(sets)};
            }
            return Join(children, column, op, op == EOp::Union ? " ∪ " : " ∩ ");
        }
        if (node->IsCallable("RangeMultiply") && node->ChildrenSize() > 1) {
            TVector<TDescription> children;
            TVector<size_t> columns;
            size_t next = column;
            for (size_t i = 1; i < node->ChildrenSize(); ++i) {
                auto description = DescribeNode(node->ChildPtr(i), next);
                if (!description) {
                    return std::nullopt;
                }
                const size_t width = description->Width;
                // Columns past a constrained prefix are padded with the full range, which says nothing.
                if (!description->Unconstrained || children.empty()) {
                    columns.push_back(next);
                    children.push_back(std::move(*description));
                }
                next += width;
            }
            if (children.size() == 1) {
                children.front().Width = next - column;
                return std::move(children.front());
            }
            TDescription result{.Width = next - column, .Op = EOp::Product};
            TStringBuilder text;
            for (size_t i = 0; i < children.size(); ++i) {
                text << (i ? " × " : "") << Render(children[i], columns[i], EOp::Product);
            }
            result.Text = text;
            return result;
        }
        return std::nullopt;
    }

    TDescription Join(const TVector<TDescription>& children, size_t column, EOp op, TStringBuf separator) const {
        TStringBuilder text;
        for (size_t i = 0; i < children.size(); ++i) {
            text << (i ? separator : "") << Render(children[i], column, op);
        }
        return TDescription{.Width = children.front().Width, .Text = text, .Op = op};
    }

    const TConstArrayRef<TString> KeyColumns_;
};

} // namespace

std::optional<TString> DescribeRangesStructure(const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns) {
    if (!ranges || keyColumns.empty()) {
        return std::nullopt;
    }
    return TRangesDescriber(keyColumns).Describe(ranges);
}

} // namespace NKikimr::NKqp::NOpt::NReadRanges
