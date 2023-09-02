#ifndef CONSTRAINTS_INL_H_
#error "Direct inclusion of this file is not allowed, include constraints.h"
// For the sake of sane code completion.
#include "constraints.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

inline bool operator < (const TValueBound& lhs, const TValueBound& rhs)
{
    return std::tie(lhs.Value, lhs.Flag) < std::tie(rhs.Value, rhs.Flag);
}

inline bool operator <= (const TValueBound& lhs, const TValueBound& rhs)
{
    return std::tie(lhs.Value, lhs.Flag) <= std::tie(rhs.Value, rhs.Flag);
}

inline bool operator == (const TValueBound& lhs, const TValueBound& rhs)
{
    return std::tie(lhs.Value, lhs.Flag) == std::tie(rhs.Value, rhs.Flag);
}

inline bool TestValue(TValue value, const TValueBound& lower, const TValueBound& upper)
{
    return lower < TValueBound{value, true} && TValueBound{value, false} < upper;
}

////////////////////////////////////////////////////////////////////////////////

inline TConstraintRef TConstraintRef::Empty()
{
    TConstraintRef result;
    result.ColumnId = 0;
    return result;
}

inline TConstraintRef TConstraintRef::Universal()
{
    return {};
}

////////////////////////////////////////////////////////////////////////////////

inline TValueBound TConstraint::GetLowerBound() const
{
    return {LowerValue, !LowerIncluded};
}

inline TValueBound TConstraint::GetUpperBound() const
{
    return {UpperValue, UpperIncluded};
}

inline TConstraint TConstraint::Make(TValueBound lowerBound, TValueBound upperBound, TConstraintRef next)
{
    YT_VERIFY(lowerBound < upperBound);
    return {
        lowerBound.Value,
        upperBound.Value,
        next,
        !lowerBound.Flag,
        upperBound.Flag};
}

////////////////////////////////////////////////////////////////////////////////

inline TValue TColumnConstraint::GetValue() const
{
    YT_ASSERT(IsExact());
    return Lower.Value;
}

inline bool TColumnConstraint::IsExact() const
{
    return Lower.Value == Upper.Value && !Lower.Flag && Upper.Flag;
}

inline bool TColumnConstraint::IsRange() const
{
    return Lower.Value < Upper.Value;
}

inline bool TColumnConstraint::IsUniversal() const
{
    return Lower.Value.Type == EValueType::Min && Upper.Value.Type == EValueType::Max;
}

////////////////////////////////////////////////////////////////////////////////

template <class TOnRange>
void TReadRangesGenerator::GenerateReadRanges(TConstraintRef constraintRef, const TOnRange& onRange, ui64 rangeExpansionLimit)
{
    auto columnId = constraintRef.ColumnId;
    if (columnId == SentinelColumnId) {
        // Leaf node.
        onRange(Row_, rangeExpansionLimit);
        return;
    }

    auto intervals = MakeRange(Constraints_[columnId])
        .Slice(constraintRef.StartIndex, constraintRef.EndIndex);

    if (rangeExpansionLimit < intervals.Size()) {
        Row_[columnId] = TColumnConstraint{intervals.Front().GetLowerBound(), intervals.Back().GetUpperBound()};

        onRange(Row_, rangeExpansionLimit);
    } else if (!intervals.Empty()) {
        ui64 nextRangeExpansionLimit = rangeExpansionLimit / intervals.Size();
        YT_VERIFY(nextRangeExpansionLimit > 0);
        for (const auto& item : intervals) {
            Row_[columnId] = TColumnConstraint{item.GetLowerBound(), item.GetUpperBound()};

            Row_[columnId].Lower.Value.Id = columnId;
            Row_[columnId].Upper.Value.Id = columnId;

            GenerateReadRanges(item.Next, onRange, nextRangeExpansionLimit);
        }
    }

    Row_[columnId] = UniversalInterval;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
