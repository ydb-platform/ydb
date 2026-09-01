#include "kqp_rbo_utils.h"
#include "kqp_operator.h"

namespace NKikimr {
namespace NKqp {

using namespace NYql;

namespace {

constexpr TStringBuf IgnoreArgPrefix = "__kqp_rbo_ignore_arg_";

} // namespace

const TInfoUnitSet& EmptyInfoUnitSet() {
    static const TInfoUnitSet empty;
    return empty;
}

bool ContainsInfoUnit(const TVector<TInfoUnit>& units, const TInfoUnit& unit) {
    return std::find(units.begin(), units.end(), unit) != units.end();
}

bool AddInfoUnit(TInfoUnitSet& target, const TInfoUnit& iu) {
    return target.insert(iu).second;
}

bool AddInfoUnits(TInfoUnitSet& target, const TVector<TInfoUnit>& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= AddInfoUnit(target, iu);
    }
    return changed;
}

bool AddInfoUnits(TInfoUnitSet& target, const TInfoUnitSet& ius) {
    bool changed = false;
    for (const auto& iu : ius) {
        changed |= AddInfoUnit(target, iu);
    }
    return changed;
}

TInfoUnitSet MakeInfoUnitSet(const TVector<TInfoUnit>& ius) {
    TInfoUnitSet result;
    AddInfoUnits(result, ius);
    return result;
}

bool IsGeneratedIgnoreIU(const TInfoUnit& iu) {
    return iu.GetAlias().empty() && iu.GetColumnName().StartsWith(IgnoreArgPrefix);
}

TInfoUnit MakeGeneratedIgnoreIU(TPlanProps& props) {
    TStringBuilder name;
    name << IgnoreArgPrefix << props.InternalVarIdx++;
    return TInfoUnit(TString(name));
}

TVector<TInfoUnit> GetSubplanResultIUs(const TIntrusivePtr<IOperator>& op) {
    if (!op) {
        return {};
    }

    if (op->Kind == EOperator::Map) {
        TVector<TInfoUnit> result;
        for (const auto& mapElement : CastOperator<TOpMap>(op)->MapElements) {
            const auto element = mapElement.GetElementName();
            if (!IsGeneratedIgnoreIU(element)) {
                result.push_back(element);
            }
        }
        if (!result.empty()) {
            return result;
        }
    }

    if (op->Kind == EOperator::Filter || op->Kind == EOperator::AddDependencies || op->Kind == EOperator::Limit || op->Kind == EOperator::Sort) {
        return GetSubplanResultIUs(CastOperator<IUnaryOperator>(op)->GetInput());
    }

    return op->GetOutputIUs();
}

bool JoinOutputsLeft(const TString& joinKind) {
    return joinKind != "RightOnly" && joinKind != "RightSemi";
}

bool JoinOutputsRight(const TString& joinKind) {
    return joinKind != "LeftOnly" && joinKind != "LeftSemi";
}

TString GetValidJoinKind(const TString& joinKind) {
    const auto joinKindLowered = to_lower(joinKind);
    if (joinKindLowered == "left") {
        return "Left";
    } else if (joinKindLowered == "inner") {
        return "Inner";
    } else if (joinKindLowered == "cross") {
        return "Cross";
    }
    return joinKind;
}

bool CanEliminateAggregateShuffle(const TOpAggregate& aggregate, const TRBOContext& ctx) {
    if (aggregate.KeyColumns.empty() || aggregate.IsDistinctAll()) {
        return false;
    }

    const bool enableShuffleElimination = ctx.KqpCtx.Config->OptShuffleEliminationForAggregation.Get()
        .GetOrElse(ctx.KqpCtx.Config->GetDefaultEnableShuffleEliminationForAggregation());
    if (!enableShuffleElimination) {
        return false;
    }

    const auto& input = aggregate.GetInput();
    if (!input->Props.Metadata || input->Props.Metadata->ShuffledByColumns.empty()) {
        return false;
    }

    // Example: input partitioned by {id} needs no reshuffle for GROUP BY {id, date},
    // because every group has a single id and is already colocated.
    return IUIsSubset(input->Props.Metadata->ShuffledByColumns, aggregate.KeyColumns);
}

TVector<TInfoUnit> IUSetDiff(TVector<TInfoUnit> left, TVector<TInfoUnit> right) {
    TVector<TInfoUnit> res;
    for (const auto& unit : left) {
        if (std::find(right.begin(), right.end(), unit) == right.end()) {
            if (std::find(res.begin(), res.end(), unit) == res.end()) {
                res.push_back(unit);
            }
        }
    }
    return res;
}

TVector<TInfoUnit> IUSetIntersect(TVector<TInfoUnit> left, TVector<TInfoUnit> right) {
    TVector<TInfoUnit> res;
    for (const auto& unit : left) {
        if (std::find(right.begin(), right.end(), unit) != right.end()) {
            if (std::find(res.begin(), res.end(), unit) == res.end()) {
                res.push_back(unit);
            }
        }
    }
    return res;
}

TVector<TInfoUnit> IUSetIntersect(TVector<TInfoUnit> left, const TInfoUnitSet& right) {
    TVector<TInfoUnit> res;
    for (const auto& unit : left) {
        if (right.contains(unit)) {
            if (std::find(res.begin(), res.end(), unit) == res.end()) {
                res.push_back(unit);
            }
        }
    }
    return res;
}

TVector<TInfoUnit> IUSetUnion(TVector<TInfoUnit> left, TVector<TInfoUnit> right) {
    TVector<TInfoUnit> res;
    for (const auto& unit : left) {
        if (std::find(res.begin(), res.end(), unit) == res.end()) {
            res.push_back(unit);
        }
    }
    for (const auto& unit : right) {
        if (std::find(res.begin(), res.end(), unit) == res.end()) {
            res.push_back(unit);
        }
    }
    return res;
}

bool IUIsSubset(TVector<TInfoUnit> left, TVector<TInfoUnit> right) {
    return IUSetDiff(left, right).empty();
}

bool SortMatchesKeyOrder(const TVector<TString>& sortColumns, const TVector<TString>& keyColumns, size_t pointPrefixLen) {
    if (sortColumns.empty() || pointPrefixLen > keyColumns.size()) {
        return false;
    }

    const THashSet<TString> pointKeys(keyColumns.begin(), keyColumns.begin() + pointPrefixLen);
    size_t next = pointPrefixLen;
    for (const auto& sortColumn : sortColumns) {
        if (sortColumn.empty()) {
            return false;
        }
        if (pointKeys.contains(sortColumn)) {
            continue;
        }
        if (next >= keyColumns.size() || keyColumns[next] != sortColumn) {
            return false;
        }
        ++next;
    }
    return true;
}

}
}
