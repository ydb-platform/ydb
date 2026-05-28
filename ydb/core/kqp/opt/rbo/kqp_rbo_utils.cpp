#include "kqp_rbo_utils.h"
#include "kqp_operator.h"

namespace NKikimr {
namespace NKqp {

using namespace NYql;

namespace {

constexpr TStringBuf IgnoreArgPrefix = "__kqp_rbo_ignore_arg_";

} // namespace

bool IsGeneratedIgnoreIU(const TInfoUnit& iu) {
    return iu.GetAlias().empty() && iu.GetColumnName().StartsWith(IgnoreArgPrefix);
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

}
}
