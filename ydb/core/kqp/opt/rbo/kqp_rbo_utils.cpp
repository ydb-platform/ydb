#include "kqp_rbo_utils.h"
#include "kqp_operator.h"

#include <tuple>

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

bool HasOutputConflicts(const TVector<TInfoUnit>& outputIUs) {
    TInfoUnitSet seen;
    for (const auto& iu : outputIUs) {
        if (!seen.insert(iu).second) {
            return true;
        }
    }
    return false;
}

bool CanExposeOutput(IOperator* op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props) {
    if (HasOutputConflicts(outputIUs)) {
        return false;
    }

    for (const auto& [parent, childIdx] : op->Parents) {
        const auto& forbidden = props.NameConstraints.GetForbiddenOut(parent, childIdx, op);
        for (const auto& iu : outputIUs) {
            if (forbidden.contains(iu)) {
                return false;
            }
        }
    }

    return true;
}

bool CanExposeOutput(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props) {
    return CanExposeOutput(op.get(), outputIUs, props);
}

bool CanExposeToParents(IOperator* op, const TPlanProps& props, THashSet<IOperator*>& visited) {
    if (!op || !visited.insert(op).second) {
        return true;
    }

    if (!CanExposeOutput(op, op->GetOutputIUs(), props)) {
        return false;
    }

    for (const auto& [parent, _] : op->Parents) {
        if (!CanExposeToParents(parent, props, visited)) {
            return false;
        }
    }

    return true;
}

bool CanExposeToParents(IOperator* op, const TPlanProps& props) {
    THashSet<IOperator*> visited;
    return CanExposeToParents(op, props, visited);
}

bool CanReplaceInParents(
    const TIntrusivePtr<IOperator>& oldOp,
    const TIntrusivePtr<IOperator>& replacement,
    const TPlanProps& props)
{
    if (!CanExposeOutput(oldOp, replacement->GetOutputIUs(), props)) {
        return false;
    }

    TVector<std::tuple<IOperator*, ui32, TIntrusivePtr<IOperator>>> oldChildren;
    oldChildren.reserve(oldOp->Parents.size());
    for (const auto& [parent, childIdx] : oldOp->Parents) {
        oldChildren.emplace_back(parent, childIdx, parent->Children[childIdx]);
        parent->Children[childIdx] = replacement;
    }

    bool valid = true;
    THashSet<IOperator*> visited;
    for (const auto& [parent, _] : oldOp->Parents) {
        if (!CanExposeToParents(parent, props, visited)) {
            valid = false;
            break;
        }
    }

    for (const auto& [parent, childIdx, oldChild] : oldChildren) {
        parent->Children[childIdx] = oldChild;
    }

    return valid;
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

bool IUIsSubset(TVector<TInfoUnit> left, TVector<TInfoUnit> right) {
    return IUSetDiff(left, right).empty();
}

}
}
