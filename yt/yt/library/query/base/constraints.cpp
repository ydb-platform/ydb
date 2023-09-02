#include "constraints.h"
#include "query.h"
#include "functions.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

// Min is included because lower bound is included.
// Max is excluded beacuse upper bound is excluded.
// It allows keep resulting ranges without augmenting suffix with additional sentinels (e.g. [Max included] -> [Max included, Max excluded]).
TValueBound MinBound{MakeUnversionedSentinelValue(EValueType::Min), false};
TValueBound MaxBound{MakeUnversionedSentinelValue(EValueType::Max), false};

TColumnConstraint UniversalInterval{MinBound, MaxBound};

////////////////////////////////////////////////////////////////////////////////

TConstraintRef TConstraintsHolder::Append(std::initializer_list<TConstraint> constraints, ui32 keyPartIndex)
{
    auto& columnConstraints = (*this)[keyPartIndex];

    TConstraintRef result;

    result.ColumnId = keyPartIndex;
    result.StartIndex = columnConstraints.size();
    result.EndIndex = result.StartIndex + constraints.size();

    columnConstraints.insert(columnConstraints.end(), constraints.begin(), constraints.end());

    return result;
}

TConstraintRef TConstraintsHolder::Interval(TValueBound lower, TValueBound upper, ui32 keyPartIndex)
{
    return Append({TConstraint::Make(lower, upper)}, keyPartIndex);
}

void VerifyConstraintsAreSorted(const TConstraintsHolder& holder, TConstraintRef ref)
{
    if (ref.ColumnId == SentinelColumnId) {
        return;
    }

    const auto& columnConstraints = holder[ref.ColumnId];

    for (ui32 index = ref.StartIndex + 1; index < ref.EndIndex; ++index) {
        YT_VERIFY(columnConstraints[index - 1].GetUpperBound() <= columnConstraints[index].GetLowerBound());
    }
}

TConstraintRef TConstraintsHolder::Intersect(TConstraintRef lhs, TConstraintRef rhs)
{
    VerifyConstraintsAreSorted(*this, lhs);
    VerifyConstraintsAreSorted(*this, rhs);

    if (lhs.ColumnId > rhs.ColumnId) {
        std::swap(lhs, rhs);
    }

    if (lhs.ColumnId < rhs.ColumnId) {
        // Intersection of lhs.Next with rhs for lhs bounds.

        auto& columnConstraints = (*this)[lhs.ColumnId];

        TConstraintRef result;
        result.ColumnId = lhs.ColumnId;
        result.StartIndex = columnConstraints.size();

        for (auto lhsIndex = lhs.StartIndex; lhsIndex != lhs.EndIndex; ++lhsIndex) {
            const auto& lhsItem = columnConstraints[lhsIndex];
            auto next = Intersect(lhsItem.Next, rhs);

            if (next.ColumnId == SentinelColumnId || next.StartIndex != next.EndIndex) {
                columnConstraints.push_back(TConstraint::Make(
                    lhsItem.GetLowerBound(),
                    lhsItem.GetUpperBound(),
                    next));
            }
        }

        result.EndIndex = columnConstraints.size();
        return result;
    }

    YT_VERIFY(lhs.ColumnId == rhs.ColumnId);

    if (lhs.ColumnId == SentinelColumnId) {
        return TConstraintRef::Universal();
    }

    auto& columnConstraints = (*this)[lhs.ColumnId];

    TConstraintRef result;
    result.ColumnId = lhs.ColumnId;
    result.StartIndex = columnConstraints.size();

    auto lhsIndex = lhs.StartIndex;
    auto rhsIndex = rhs.StartIndex;

    while (lhsIndex != lhs.EndIndex && rhsIndex != rhs.EndIndex) {
        const auto& lhsItem = columnConstraints[lhsIndex];
        const auto& rhsItem = columnConstraints[rhsIndex];

        // Keep by values because columnConstraints can be reallocated in .push_back method.
        auto lhsLower = lhsItem.GetLowerBound();
        auto lhsUpper = lhsItem.GetUpperBound();
        auto rhsLower = rhsItem.GetLowerBound();
        auto rhsUpper = rhsItem.GetUpperBound();

        auto lhsNext = lhsItem.Next;
        auto rhsNext = rhsItem.Next;

        auto intersectionLower = std::max(lhsLower, rhsLower);
        auto intersectionUpper = std::min(lhsUpper, rhsUpper);

        if (intersectionLower < intersectionUpper) {
            auto next = Intersect(lhsNext, rhsNext);
            if (next.ColumnId == SentinelColumnId || next.StartIndex != next.EndIndex) {
                columnConstraints.push_back(TConstraint::Make(
                    intersectionLower,
                    intersectionUpper,
                    next));
            }
        }

        if (lhsUpper < rhsUpper) {
            ++lhsIndex;
        } else if (rhsUpper < lhsUpper) {
            ++rhsIndex;
        } else {
            ++lhsIndex;
            ++rhsIndex;
        }
    }

    result.EndIndex = columnConstraints.size();
    return result;
}

TConstraintRef TConstraintsHolder::Unite(TConstraintRef lhs, TConstraintRef rhs)
{
    VerifyConstraintsAreSorted(*this, lhs);
    VerifyConstraintsAreSorted(*this, rhs);

    if (lhs.ColumnId > rhs.ColumnId) {
        std::swap(lhs, rhs);
    }

    if (lhs.ColumnId < rhs.ColumnId) {
        // Union of lhs.Next with rhs for lhs bounds and rhs for complement of lhs bounds.

        // Treat skipped column as universal constraint.
        rhs = TConstraintsHolder::Append({
            TConstraint::Make(
                MinBound,
                MaxBound,
                rhs)
            },
            lhs.ColumnId);
    }

    YT_VERIFY(lhs.ColumnId == rhs.ColumnId);

    if (lhs.ColumnId == SentinelColumnId) {
        return TConstraintRef::Universal();
    }

    auto& columnConstraints = (*this)[lhs.ColumnId];

    TConstraintRef result;
    result.ColumnId = lhs.ColumnId;
    result.StartIndex = columnConstraints.size();

    auto lhsIndex = lhs.StartIndex;
    auto rhsIndex = rhs.StartIndex;

    TValueBound lastBound{MakeUnversionedSentinelValue(EValueType::Min), false};

    while (lhsIndex != lhs.EndIndex && rhsIndex != rhs.EndIndex) {
        const auto& lhsItem = columnConstraints[lhsIndex];
        const auto& rhsItem = columnConstraints[rhsIndex];

        // Keep by values because columnConstraints can be reallocated in .push_back method.
        auto lhsLower = lhsItem.GetLowerBound();
        auto lhsUpper = lhsItem.GetUpperBound();
        auto rhsLower = rhsItem.GetLowerBound();
        auto rhsUpper = rhsItem.GetUpperBound();

        auto lhsNext = lhsItem.Next;
        auto rhsNext = rhsItem.Next;

        // Unite [a, b] and [c, d]
        // Cases:
        // a b c d
        // [ ]
        //     [ ]
        // a c d b
        // [     ]
        //   [ ]
        // a c b d
        // [   ]
        //   [   ]

        // Disjoint. Append lhs.
        if (lhsUpper <= rhsLower) {
            columnConstraints.push_back(TConstraint::Make(
                std::max(lastBound, lhsLower),
                lhsUpper,
                lhsNext));

            ++lhsIndex;
            continue;
        }

        // Disjoint. Append rhs.
        if (rhsUpper <= lhsLower) {
            columnConstraints.push_back(TConstraint::Make(
                std::max(lastBound, rhsLower),
                rhsUpper,
                rhsNext));
            ++rhsIndex;
            continue;
        }

        auto intersectionLower = std::max(lhsLower, rhsLower);
        auto intersectionUpper = std::min(lhsUpper, rhsUpper);

        auto unionLower = std::max(lastBound, lhsLower < rhsLower ? lhsLower : rhsLower);
        lastBound = intersectionUpper;

        if (unionLower < intersectionLower) {
            columnConstraints.push_back(TConstraint::Make(
                unionLower,
                intersectionLower,
                lhsLower < rhsLower ? lhsNext : rhsNext));
        }

        YT_VERIFY(intersectionLower < intersectionUpper);

        auto next = Unite(lhsNext, rhsNext);

        columnConstraints.push_back(
            TConstraint::Make(intersectionLower, intersectionUpper, next));

        if (lhsUpper < rhsUpper) {
            ++lhsIndex;
        } else if (rhsUpper < lhsUpper) {
            ++rhsIndex;
        } else {
            ++lhsIndex;
            ++rhsIndex;
        }
    }

    while (lhsIndex != lhs.EndIndex) {
        const auto& lhsItem = columnConstraints[lhsIndex];

        auto lowerBound = std::max(lastBound, lhsItem.GetLowerBound());
        auto upperBound = lhsItem.GetUpperBound();
        auto lhsNext = lhsItem.Next;

        if (lowerBound < upperBound) {
            columnConstraints.push_back(TConstraint::Make(
                lowerBound,
                upperBound,
                lhsNext));
        }
        ++lhsIndex;
    }

    while (rhsIndex != rhs.EndIndex) {
        const auto& rhsItem = columnConstraints[rhsIndex];

        auto lowerBound = std::max(lastBound, rhsItem.GetLowerBound());
        auto upperBound = rhsItem.GetUpperBound();
        auto rhsNext = rhsItem.Next;

        if (lowerBound < upperBound) {
            columnConstraints.push_back(TConstraint::Make(
                lowerBound,
                upperBound,
                rhsNext));
        }
        ++rhsIndex;
    }

    result.EndIndex = columnConstraints.size();
    return result;
}

TString ToString(const TConstraintsHolder& constraints, TConstraintRef root)
{
    TStringBuilder result;

    result.AppendString("Constraints:");

    auto addOffset = [&] (int offset) {
        for (int i = 0; i < offset; ++i) {
            result.AppendString(". ");
        }
        return result;
    };

    std::function<void(TConstraintRef)> printNode =
        [&] (TConstraintRef ref) {
            if (ref.ColumnId == SentinelColumnId) {
                result.AppendString(" <universe>");
            } else {
                if (ref.StartIndex == ref.EndIndex) {
                    result.AppendString(" <empty>");
                    return;
                }

                for (const auto& item : MakeRange(constraints[ref.ColumnId]).Slice(ref.StartIndex, ref.EndIndex)) {
                    result.AppendString("\n");
                    addOffset(ref.ColumnId);
                    TColumnConstraint columnConstraint{item.GetLowerBound(), item.GetUpperBound()};

                    if (columnConstraint.IsExact()) {
                        result.AppendFormat("%kv", columnConstraint.GetValue());
                    } else {
                        result.AppendFormat("%v%kv .. %kv%v",
                            "[("[columnConstraint.Lower.Flag],
                            columnConstraint.Lower.Value,
                            columnConstraint.Upper.Value,
                            ")]"[columnConstraint.Upper.Flag]);
                    }

                    result.AppendString(":");
                    printNode(item.Next);
                }
            }
        };

    printNode(root);
    return result.Flush();
}

TReadRangesGenerator::TReadRangesGenerator(const TConstraintsHolder& constraints)
    : Constraints_(constraints)
    , Row_(Constraints_.size(), UniversalInterval)
{ }

static void CopyValues(TRange<TValue> source, TMutableRow dest)
{
    std::copy(source.Begin(), source.End(), dest.Begin());
}

TMutableRow MakeLowerBound(TRowBuffer* rowBuffer, TRange<TValue> boundPrefix, TValueBound lastBound)
{
    auto prefixSize = boundPrefix.Size();

    if (lastBound.Value.Type == EValueType::Min) {
        auto lowerBound = rowBuffer->AllocateUnversioned(prefixSize);
        CopyValues(boundPrefix, lowerBound);
        return lowerBound;
    }

    // Consider included/excluded bounds.
    bool lowerExcluded = lastBound.Flag;
    auto lowerBound = rowBuffer->AllocateUnversioned(prefixSize + 1 + lowerExcluded);
    CopyValues(boundPrefix, lowerBound);
    lowerBound[prefixSize] = lastBound.Value;
    if (lowerExcluded) {
        lowerBound[prefixSize + 1] = MakeUnversionedSentinelValue(EValueType::Max);
    }
    return lowerBound;
}

TMutableRow MakeUpperBound(TRowBuffer* rowBuffer, TRange<TValue> boundPrefix, TValueBound lastBound)
{
    auto prefixSize = boundPrefix.Size();

    // Consider included/excluded bounds.
    bool upperIncluded = lastBound.Flag;
    auto upperBound = rowBuffer->AllocateUnversioned(prefixSize + 1 + upperIncluded);
    CopyValues(boundPrefix, upperBound);
    upperBound[prefixSize] = lastBound.Value;
    if (upperIncluded) {
        upperBound[prefixSize + 1] = MakeUnversionedSentinelValue(EValueType::Max);
    }
    return upperBound;
}

TRowRange RowRangeFromPrefix(TRowBuffer* rowBuffer, TRange<TValue> boundPrefix)
{
    auto prefixSize = boundPrefix.Size();

    auto lowerBound = rowBuffer->AllocateUnversioned(prefixSize);
    CopyValues(boundPrefix, lowerBound);

    auto upperBound = rowBuffer->AllocateUnversioned(prefixSize + 1);
    CopyValues(boundPrefix, upperBound);
    upperBound[prefixSize] = MakeUnversionedSentinelValue(EValueType::Max);
    return std::make_pair(lowerBound, upperBound);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
