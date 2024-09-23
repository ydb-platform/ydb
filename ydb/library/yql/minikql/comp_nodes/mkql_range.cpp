#include "mkql_range.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/presort.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <queue>
#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql::NUdf;

namespace {

using TTypeList = std::vector<TType*>;
using TUnboxedValueQueue = std::deque<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue>>;

struct TRangeTypeInfo {
    TType* RangeType = nullptr;
    ICompare::TPtr RangeCompare;

    TType* BoundaryType = nullptr;
    ICompare::TPtr BoundaryCompare;

    TTypeList Components;
    std::vector<ICompare::TPtr> ComponentsCompare;
};

TType* RemoveAllOptionals(TType* type) {
    Y_ENSURE(type);
    while (type->IsOptional()) {
        type = static_cast<TOptionalType*>(type)->GetItemType();
    }
    return type;
}

TRangeTypeInfo ExtractTypes(TType* rangeType) {
    TRangeTypeInfo result;
    result.RangeType = rangeType;
    result.RangeCompare = MakeCompareImpl(result.RangeType);

    MKQL_ENSURE(result.RangeType->IsTuple(), "Expecting range to be of tuple type");
    auto rangeTupleType = static_cast<TTupleType*>(result.RangeType);
    MKQL_ENSURE(rangeTupleType->GetElementsCount() == 2, "Expecting range to be of tuple type with 2 elements");
    MKQL_ENSURE(rangeTupleType->GetElementType(0)->IsSameType(*rangeTupleType->GetElementType(1)),
        "Expecting range to be of tuple type with 2 elements of same type");

    result.BoundaryType = rangeTupleType->GetElementType(0);
    result.BoundaryCompare = MakeCompareImpl(result.BoundaryType);

    MKQL_ENSURE(result.BoundaryType->IsTuple(), "Expecting range boundary to be of tuple type");
    auto rangeBoundaryTupleType = static_cast<TTupleType*>(result.BoundaryType);
    MKQL_ENSURE(rangeBoundaryTupleType->GetElementsCount() >= 3,
        "Expecting range boundary to be of tuple type with at least 3 elements");

    MKQL_ENSURE(rangeBoundaryTupleType->GetElementsCount() % 2 == 1,
        "Expecting range boundary to be of tuple type with odd element count");

    for (ui32 i = 0; i < rangeBoundaryTupleType->GetElementsCount(); ++i) {
        auto type = rangeBoundaryTupleType->GetElementType(i);
        if (i % 2 == 1) {
            auto baseType = RemoveAllOptionals(type);
            MKQL_ENSURE(type->IsOptional() && (baseType->IsData() || baseType->IsPg()),
                "Expecting (multiple) optional of Data or Pg at odd positions of range boundary tuple");
        } else {
            MKQL_ENSURE(type->IsData() && static_cast<TDataType*>(type)->GetSchemeType() == NUdf::TDataType<i32>::Id,
                "Expected i32 at even positions of range boundary tuple");
        }
        result.Components.push_back(type);
        result.ComponentsCompare.push_back(MakeCompareImpl(type));
    }
    return result;
}

struct TExpandedRangeBoundary {
    int Included = 0;          // -1 = [; 0 = (); +1 = ]
    TUnboxedValue Value;       // AsTuple(Inf, x, Inf, y, Inf, z, ..., Included), where -1 = -inf, +1 = +inf, 0 - finite value
    TUnboxedValueVector Components;
};

struct TExpandedRange {
    TExpandedRangeBoundary Left;
    TExpandedRangeBoundary Right;
};

TExpandedRangeBoundary Max(TExpandedRangeBoundary a, TExpandedRangeBoundary b, ICompare* cmp) {
    return cmp->Less(a.Value, b.Value) ? b : a;
}

TExpandedRangeBoundary Min(TExpandedRangeBoundary a, TExpandedRangeBoundary b, ICompare* cmp) {
    return cmp->Less(a.Value, b.Value) ? a : b;
}

i32 GetInfSign(bool hasPrefix, bool isIncluded, bool isLeft) {
    if (!hasPrefix || isIncluded) {
        return (isLeft ? -1 : 1);
    }
    return (isLeft ? 1 : -1);
}

TExpandedRangeBoundary ExpandRangeBoundary(TUnboxedValue value, bool left) {
    auto elements = value.GetElements();
    auto elementsCount = value.GetListLength();

    Y_ENSURE(elements);
    Y_ENSURE(elementsCount >= 3 && elementsCount % 2 == 1);

    TExpandedRangeBoundary result;
    result.Value = value;
    const bool hasPrefix = elements[0].Get<i32>() == 0;
    const bool isIncluded = elements[elementsCount - 1].Get<i32>() != 0;
    for (size_t i = 0; i < elementsCount - 1; i += 2) {
        i32 inf = elements[i].Get<i32>();
        MKQL_ENSURE(inf == 0 || inf == GetInfSign(hasPrefix, isIncluded, left),
            "Invalid value for range boundary inf marker: " << inf << " at position " << i);
        MKQL_ENSURE((inf != 0) ^ bool(elements[i + 1]),
            "Value does not match inf marker: " << inf << " at position " << i);
    }
    result.Components.assign(elements, elements + elementsCount);
    result.Included = result.Components.back().Get<i32>();
    MKQL_ENSURE(!result.Included || result.Included == (left ? -1 : 1),
        "Invalid value for range boundary last element: " << result.Included);
    return result;
}

TExpandedRange ExpandRange(TUnboxedValue value) {
    auto elements = value.GetElements();
    auto elementsCount = value.GetListLength();

    Y_ENSURE(elements);
    Y_ENSURE(elementsCount == 2);


    TExpandedRange result;
    result.Left = ExpandRangeBoundary(elements[0], true);
    result.Right = ExpandRangeBoundary(elements[1], false);

    Y_ENSURE(result.Left.Components.size() == result.Right.Components.size());
    bool seenInfRange = false;
    for (size_t i = 0; i < result.Left.Components.size() - 1; i += 2) {
        if (result.Left.Components[i].Get<i32>() && result.Right.Components[i].Get<i32>()) {
            seenInfRange = true;
        } else {
            MKQL_ENSURE(!seenInfRange, "Non inf component follows inf component at position " << i);
        }
    }
    return result;
}

size_t GetFiniteComponentsCount(const TExpandedRangeBoundary& boundary) {
    size_t result = 0;
    for (size_t i = 0; i < boundary.Components.size() - 1; i += 2) {
        if (boundary.Components[i].Get<i32>() != 0) {
            break;
        }
        result += 1;
    }
    return result;
}

template<typename T>
bool IsAdjacentNumericValues(TUnboxedValue left, TUnboxedValue right) {
    T l = left.Get<T>();
    T r = right.Get<T>();
    Y_ENSURE(l < r);
    return l + 1 == r;
}

bool CanConvertToPointRange(const TExpandedRange& range, const TRangeTypeInfo& typeInfo) {
    if (!(range.Left.Included && !range.Right.Included ||
          !range.Left.Included && range.Right.Included))
    {
        return false;
    }
    const size_t compsCount = GetFiniteComponentsCount(range.Left);
    if (compsCount == 0 || GetFiniteComponentsCount(range.Right) != compsCount) {
        return false;
    }

    const size_t lastCompIdx = 2 * (compsCount - 1) + 1;

    // check for suitable type
    TType* baseType = RemoveAllOptionals(static_cast<TTupleType*>(typeInfo.BoundaryType)->GetElementType(lastCompIdx));
    auto slot = baseType->IsData() ? static_cast<TDataType*>(baseType)->GetDataSlot() : TMaybe<EDataSlot>{};
    if (!slot || !(GetDataTypeInfo(*slot).Features & (NUdf::EDataTypeFeatures::IntegralType | NUdf::EDataTypeFeatures::DateType))) {
        return false;
    }

    // all components before last should be equal
    for (size_t i = 1; i < lastCompIdx; i += 2) {
        if (typeInfo.ComponentsCompare[i]->Compare(range.Left.Components[i], range.Right.Components[i])) {
            return false;
        }
    }

    auto left = range.Left.Components[lastCompIdx];
    auto right = range.Right.Components[lastCompIdx];
    if (!left.HasValue() || !right.HasValue()) {
        return false;
    }

    switch (*slot) {
        case EDataSlot::Int8:   return IsAdjacentNumericValues<i8>(left, right);
        case EDataSlot::Uint8:  return IsAdjacentNumericValues<ui8>(left, right);
        case EDataSlot::Int16:  return IsAdjacentNumericValues<i16>(left, right);
        case EDataSlot::Uint16: return IsAdjacentNumericValues<ui16>(left, right);
        case EDataSlot::Int32:  return IsAdjacentNumericValues<i32>(left, right);
        case EDataSlot::Uint32: return IsAdjacentNumericValues<ui32>(left, right);
        case EDataSlot::Int64:  return IsAdjacentNumericValues<i64>(left, right);
        case EDataSlot::Uint64: return IsAdjacentNumericValues<ui64>(left, right);

        case EDataSlot::Date:     return IsAdjacentNumericValues<ui16>(left, right);
        case EDataSlot::Date32:   return IsAdjacentNumericValues<i32>(left, right);
        case EDataSlot::Datetime: return IsAdjacentNumericValues<ui32>(left, right);
        case EDataSlot::Timestamp: return IsAdjacentNumericValues<ui64>(left, right);
        case EDataSlot::Datetime64: return IsAdjacentNumericValues<i64>(left, right);
        case EDataSlot::Timestamp64: return IsAdjacentNumericValues<i64>(left, right);
        default: break;
    }
    MKQL_ENSURE(false, "Unsupported type: " << *slot);
}

bool RangeIsEmpty(const TExpandedRange& range, const TRangeTypeInfo& typeInfo) {
    if (typeInfo.BoundaryCompare->Compare(range.Left.Value, range.Right.Value) >= 0) {
        // left >= right
        return true;
    }

    Y_ENSURE(typeInfo.ComponentsCompare.size() == range.Left.Components.size());
    // range is not empty if components are not equal
    for (size_t i = 0; i < typeInfo.ComponentsCompare.size() - 1; ++i) {
        if (typeInfo.ComponentsCompare[i]->Compare(range.Left.Components[i], range.Right.Components[i])) {
            return false;
        }
    }

    // all component are equal, and range is empty if any side is excluded
    return range.Left.Components.back().Get<i32>() == 0 || range.Right.Components.back().Get<i32>() == 0;
}

bool RangeCanMerge(const TExpandedRange& a, const TExpandedRange& b, const TRangeTypeInfo& typeInfo) {
    // It is assumed that a <= b here
    //       <       {         >        }
    //       a.Left  b.Left    a.Right  b.Right
    TExpandedRange intersection = { b.Left, a.Right };
    int cmp = typeInfo.BoundaryCompare->Compare(intersection.Left.Value, intersection.Right.Value);
    if (cmp > 0) {
        return false;
    }

    const auto& lefts = intersection.Left.Components;
    const auto& rights = intersection.Right.Components;

    bool leftIncluded = lefts.back().Get<i32>() != 0;
    bool rightIncluded = rights.back().Get<i32>() != 0;

    for (size_t i = 0; i < lefts.size() - 1; i += 2) {
        auto infCmp = typeInfo.ComponentsCompare[i].Get();
        auto compCmp = typeInfo.ComponentsCompare[i + 1].Get();

        auto infCompareRes = infCmp->Compare(lefts[i], rights[i]);
        Y_ENSURE(infCompareRes <= 0);
        if (infCompareRes < 0) {
            return true;
        }

        auto componentCompareRes = compCmp->Compare(lefts[i + 1], rights[i + 1]);
        Y_ENSURE(componentCompareRes <= 0);
        if (componentCompareRes < 0) {
            return true;
        }
    }

    return leftIncluded || rightIncluded;
}

class TRangeComputeBase {
public:
    TRangeComputeBase(TComputationMutables&, TComputationNodePtrVector&& lists, std::vector<TRangeTypeInfo>&& typeInfos)
        : Lists(std::move(lists)), TypeInfos(std::move(typeInfos))
    {
        Y_ENSURE(Lists.size() == TypeInfos.size());
        Y_ENSURE(!Lists.empty());
    }
protected:
    std::vector<TUnboxedValueQueue> ExpandLists(TComputationContext& ctx) const {
        TUnboxedValueVector lists;
        lists.reserve(Lists.size());
        for (auto& list : Lists) {
            lists.emplace_back(list->GetValue(ctx));
        }

        std::vector<TUnboxedValueQueue> expandedLists;
        for (size_t i = 0; i < lists.size(); ++i) {
            expandedLists.emplace_back();
            TThresher<false>::DoForEachItem(lists[i],
                [&] (NUdf::TUnboxedValue&& item) {
                    expandedLists.back().emplace_back(std::move(item));
                }
            );
            NormalizeRanges(expandedLists.back(), TypeInfos[i]);
        }

        return expandedLists;
    }

private:
    template<typename TContainer>
    static void NormalizeRanges(TContainer& ranges, const TRangeTypeInfo& typeInfo) {
        auto rangeLess = [&](const TUnboxedValuePod& a, const TUnboxedValuePod& b) {
            return typeInfo.RangeCompare->Less(a, b);
        };

        auto rangeEqual = [&](const TUnboxedValuePod& a, const TUnboxedValuePod& b) {
            return typeInfo.RangeCompare->Compare(a, b) == 0;
        };

        for (size_t i = 1; i < ranges.size(); ++i) {
            if (rangeLess(ranges[i], ranges[i - 1])) {
                std::sort(ranges.begin(), ranges.end(), rangeLess);
                break;
            }
        }

        ranges.erase(
            std::remove_if(ranges.begin(), ranges.end(),
                           [&](const TUnboxedValue& range) { return RangeIsEmpty(ExpandRange(range), typeInfo); }),
            ranges.end());
        ranges.erase(std::unique(ranges.begin(), ranges.end(), rangeEqual), ranges.end());
    }

protected:
    const TComputationNodePtrVector Lists;
    const std::vector<TRangeTypeInfo> TypeInfos;
};

class TRangeUnionWrapper : public TMutableComputationNode<TRangeUnionWrapper>, public TRangeComputeBase {
    typedef TMutableComputationNode<TRangeUnionWrapper> TBaseComputation;
public:
    TRangeUnionWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& lists, std::vector<TRangeTypeInfo>&& typeInfos)
        : TBaseComputation(mutables)
        , TRangeComputeBase(mutables, std::move(lists), std::move(typeInfos))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValueVector mergedLists;
        auto expandedLists = ExpandLists(ctx);

        auto comparator = [&](size_t l, size_t r) { return TypeInfos.front().RangeCompare->Less(expandedLists[r].front(), expandedLists[l].front()); };
        std::priority_queue<size_t, std::vector<size_t>, decltype(comparator)> queue{comparator};
        for (size_t i = 0; i < expandedLists.size(); ++i) {
            if (!expandedLists[i].empty()) {
                queue.push(i);
            }
        }

        while (!queue.empty()) {
            auto argMin = queue.top();
            queue.pop();

            auto& from = expandedLists[argMin];
            if (!RangeIsEmpty(ExpandRange(from.front()), TypeInfos.front())) {
                mergedLists.emplace_back(std::move(from.front()));
            }
            from.pop_front();

            if (!from.empty()) {
                queue.push(argMin);
            }
        }

        TUnboxedValueVector unionList;
        if (!mergedLists.empty()) {
            unionList.push_back(mergedLists.front());
            auto current = ExpandRange(unionList.back());
            for (size_t i = 1; i < mergedLists.size(); ++i) {
                auto toUnion = ExpandRange(mergedLists[i]);
                if (RangeCanMerge(current, toUnion, TypeInfos.front())) {
                    current = { current.Left, Max(current.Right, toUnion.Right, TypeInfos.front().BoundaryCompare.Get()) };
                    TUnboxedValueVector newValue = { current.Left.Value, current.Right.Value };
                    unionList.back() = ctx.HolderFactory.VectorAsArray(newValue);
                } else {
                    unionList.emplace_back(std::move(mergedLists[i]));
                    current = ExpandRange(unionList.back());
                }
            }
        }

        TDefaultListRepresentation res;
        for (auto& item : unionList) {
            res = res.Append(std::move(item));
        }
        return ctx.HolderFactory.CreateDirectListHolder(std::move(res));
    }

private:
    void RegisterDependencies() const final {
        std::for_each(Lists.cbegin(), Lists.cend(), std::bind(&TRangeUnionWrapper::DependsOn, this, std::placeholders::_1));
    }
};

class TRangeIntersectWrapper : public TMutableComputationNode<TRangeIntersectWrapper>, public TRangeComputeBase {
    typedef TMutableComputationNode<TRangeIntersectWrapper> TBaseComputation;
public:
    TRangeIntersectWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& lists, std::vector<TRangeTypeInfo>&& typeInfos)
        : TBaseComputation(mutables)
        , TRangeComputeBase(mutables, std::move(lists), std::move(typeInfos))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValueVector mergedLists;
        auto expandedLists = ExpandLists(ctx);
        Y_ENSURE(!expandedLists.empty());
        TUnboxedValueQueue intersected = std::move(expandedLists.front());
        for (size_t i = 1; i < expandedLists.size(); ++i) {
            DoIntersect(ctx, intersected, std::move(expandedLists[i]));
        }

        TDefaultListRepresentation res;
        for (auto& item : intersected) {
            res = res.Append(std::move(item));
        }
        return ctx.HolderFactory.CreateDirectListHolder(std::move(res));
    }

private:
    void RegisterDependencies() const final {
        std::for_each(Lists.cbegin(), Lists.cend(), std::bind(&TRangeIntersectWrapper::DependsOn, this, std::placeholders::_1));
    }

    void DoIntersect(TComputationContext& ctx, TUnboxedValueQueue& current, TUnboxedValueQueue&& next) const {
        TUnboxedValueQueue result;
        auto cmp = TypeInfos.front().RangeCompare.Get();
        auto boundaryCmp = TypeInfos.front().BoundaryCompare.Get();
        while (!current.empty() && !next.empty()) {
            TUnboxedValueQueue* minInput;
            TUnboxedValueQueue* maxInput;
            if (cmp->Less(current.front(), next.front())) {
                minInput = &current;
                maxInput = &next;
            } else {
                minInput = &next;
                maxInput = &current;
            }

            auto minRange = ExpandRange(minInput->front());
            auto maxRange = ExpandRange(maxInput->front());

            TExpandedRange intersected;
            intersected.Left = maxRange.Left;
            intersected.Right = Min(minRange.Right, maxRange.Right, TypeInfos.front().BoundaryCompare.Get());
            if (!RangeIsEmpty(intersected, TypeInfos.front())) {
                TUnboxedValueVector newValue = { intersected.Left.Value, intersected.Right.Value };
                result.push_back(ctx.HolderFactory.VectorAsArray(newValue));

                if (boundaryCmp->Less(minRange.Right.Value, maxRange.Right.Value)) {
                    minInput->pop_front();
                } else {
                    maxInput->pop_front();
                }
            } else {
                minInput->pop_front();
            }
        }
        std::swap(current, result);
    }
};

class TRangeMultiplyWrapper : public TMutableComputationNode<TRangeMultiplyWrapper>, public TRangeComputeBase {
    typedef TMutableComputationNode<TRangeMultiplyWrapper> TBaseComputation;
public:
    TRangeMultiplyWrapper(TComputationMutables& mutables, IComputationNode* limit, TComputationNodePtrVector&& lists, std::vector<TRangeTypeInfo>&& typeInfos)
        : TBaseComputation(mutables)
        , TRangeComputeBase(mutables, std::move(lists), std::move(typeInfos))
        , Limit(limit)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const ui64 limit = Limit->GetValue(ctx).Get<ui64>();
        TUnboxedValueVector mergedLists;
        auto expandedLists = ExpandLists(ctx);
        Y_ENSURE(!expandedLists.empty());
        if (expandedLists.size() == 1 && expandedLists.front().size() > limit) {
            return FullRange(ctx);
        }

        TUnboxedValueQueue current = std::move(expandedLists.front());
        std::vector<ICompare*> currentComponentsCompare;
        currentComponentsCompare.reserve(TypeInfos.front().ComponentsCompare.size());
        for (const auto& comp : TypeInfos.front().ComponentsCompare) {
            currentComponentsCompare.push_back(comp.Get());
        }
        for (size_t i = 1; i < expandedLists.size(); ++i) {
            if (expandedLists[i].empty()) {
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }
            if (!DoMultiply(ctx, limit, current, expandedLists[i], currentComponentsCompare, TypeInfos[i])) {
                if (i > 0) {
                    PadInfs(ctx, current, i);
                    break;
                } else {
                    return FullRange(ctx);
                }
            }
        }

        TDefaultListRepresentation res;
        for (auto& item : current) {
            res = res.Append(std::move(item));
        }
        return ctx.HolderFactory.CreateDirectListHolder(std::move(res));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Limit);
        std::for_each(Lists.cbegin(), Lists.cend(), std::bind(&TRangeMultiplyWrapper::DependsOn, this, std::placeholders::_1));
    }

    void PadInfs(TComputationContext& ctx, TUnboxedValueQueue& current, size_t currentPrefix) const {
        size_t extraColumns = 0;
        for (size_t i = 0; i < TypeInfos.size(); ++i) {
            const auto& ti = TypeInfos[i];
            Y_ENSURE(ti.Components.size() % 2 == 1);
            if (currentPrefix <= i) {
                extraColumns += (ti.Components.size() - 1) / 2;
            }
        }

        TUnboxedValueQueue result;
        for (const auto& c : current) {
            auto curr = ExpandRange(c);
            result.push_back(AppendInfs(ctx, curr, extraColumns));
        }
        std::swap(current, result);
    }

    bool DoMultiply(TComputationContext& ctx, ui64 limit, TUnboxedValueQueue& current, const TUnboxedValueQueue& next,
        std::vector<ICompare*>& currentCmps, const TRangeTypeInfo& nextTypeInfo) const
    {
        TUnboxedValueQueue result;
        Y_ENSURE(currentCmps.size() >= 3 && currentCmps.size() % 2 == 1);
        size_t extraColumns = (nextTypeInfo.ComponentsCompare.size() - 1) / 2;
        for (const auto& c : current) {
            auto curr = ExpandRange(c);
            if (RangeIsPoint(curr, currentCmps)) {
                if (result.size() + next.size() > limit) {
                    return false;
                }
                for (const auto& n : next) {
                    result.push_back(Append(ctx, curr, ExpandRange(n)));
                }
            } else {
                if (result.size() + 1 > limit) {
                    return false;
                }
                result.push_back(AppendInfs(ctx, curr, extraColumns));
            }
        }

        currentCmps.pop_back();
        for (const auto& comp : nextTypeInfo.ComponentsCompare) {
            currentCmps.push_back(comp.Get());
        }

        std::swap(current, result);
        return true;
    }

    static bool RangeIsPoint(const TExpandedRange& range, const std::vector<ICompare*>& cmps) {
        Y_ENSURE(range.Left.Components.size() == cmps.size());
        TUnboxedValue leftIncluded = range.Left.Components.back();
        TUnboxedValue rightIncluded = range.Right.Components.back();
        if (!leftIncluded.Get<i32>() || !rightIncluded.Get<i32>()) {
            return false;
        }

        bool allEqual = true;
        for (size_t i = 0; allEqual && i < cmps.size() - 1; ++i) {
            allEqual = allEqual &&
                cmps[i]->Compare(range.Left.Components[i], range.Right.Components[i]) == 0;
        }

        return allEqual;
    }

    static TUnboxedValuePod Append(TComputationContext& ctx, const TExpandedRange& first, const TExpandedRange& second) {
        auto left = Append(ctx, first.Left, second.Left);
        auto right = Append(ctx, first.Right, second.Right);
        TUnboxedValueVector range = { left, right };
        return ctx.HolderFactory.VectorAsArray(range);
    }

    static TUnboxedValuePod Append(TComputationContext& ctx, const TExpandedRangeBoundary& first,
        const TExpandedRangeBoundary& second)
    {
        TUnboxedValueVector components(first.Components.begin(), first.Components.end() - 1);
        components.insert(components.end(), second.Components.begin(), second.Components.end());
        if (second.Components.front().Get<i32>() != 0) {
            // preserve original include/exclude flag when appending nulls (+-inf)
            components.back() = first.Components.back();
        }
        return ctx.HolderFactory.VectorAsArray(components);
    }

    static TUnboxedValuePod AppendInfs(TComputationContext& ctx, const TExpandedRange& range, size_t count) {
        auto left = AppendInfs(ctx, true, range.Left, count);
        auto right = AppendInfs(ctx, false, range.Right, count);
        TUnboxedValueVector newRange = { left, right };
        return ctx.HolderFactory.VectorAsArray(newRange);
    }

    static TUnboxedValuePod AppendInfs(TComputationContext& ctx, bool isLeft, const TExpandedRangeBoundary& boundary, size_t count) {
        Y_ENSURE(!boundary.Components.empty());
        TUnboxedValueVector components(boundary.Components.begin(), boundary.Components.end() - 1);
        const bool hasPrefix = boundary.Components.size() > 1 && boundary.Components.front().Get<i32>() == 0;
        const bool isIncluded = boundary.Components.back().Get<i32>() != 0;
        for (size_t i = 0; i < count; ++i) {
            components.push_back(TUnboxedValuePod(GetInfSign(hasPrefix, isIncluded, isLeft)));
            components.emplace_back();
        }
        components.push_back(boundary.Components.back());
        return ctx.HolderFactory.VectorAsArray(components);
    }

    TUnboxedValuePod FullRange(TComputationContext& ctx) const {
        size_t columnCount = 0;
        for (const auto& ti : TypeInfos) {
            Y_ENSURE(ti.Components.size() % 2 == 1);
            columnCount += (ti.Components.size() - 1) / 2;
        }

        TExpandedRange range;
        range.Left.Components.push_back(TUnboxedValuePod(0));
        range.Right.Components.push_back(TUnboxedValuePod(0));
        TUnboxedValueVector result = { AppendInfs(ctx, range, columnCount) };
        return ctx.HolderFactory.VectorAsArray(result);
    }

    IComputationNode* const Limit;
};


class TRangeFinalizeWrapper : public TMutableComputationNode<TRangeFinalizeWrapper>, public TRangeComputeBase {
    typedef TMutableComputationNode<TRangeFinalizeWrapper> TBaseComputation;
public:
    TRangeFinalizeWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& lists, std::vector<TRangeTypeInfo>&& typeInfos)
        : TBaseComputation(mutables)
        , TRangeComputeBase(mutables, std::move(lists), std::move(typeInfos))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto expandedLists = ExpandLists(ctx);
        Y_ENSURE(expandedLists.size() == 1);

        TDefaultListRepresentation res;
        for (auto& item : expandedLists.front()) {
            auto range = ExpandRange(item);
            if (CanConvertToPointRange(range, TypeInfos.front())) {
                if (range.Left.Included) {
                    range.Right = range.Left;
                } else {
                    range.Left = range.Right;
                }
            }

            auto left = ConvertFromInternal(range.Left.Components, ctx);
            auto right = ConvertFromInternal(range.Right.Components, ctx);

            TUnboxedValueVector rangeVector = { left, right };
            res = res.Append(ctx.HolderFactory.VectorAsArray(rangeVector));
        }
        return ctx.HolderFactory.CreateDirectListHolder(std::move(res));
    }

private:
    void RegisterDependencies() const final {
        std::for_each(Lists.cbegin(), Lists.cend(), std::bind(&TRangeFinalizeWrapper::DependsOn, this, std::placeholders::_1));
    }

    TUnboxedValue ConvertFromInternal(const TUnboxedValueVector& boundaryComponents, TComputationContext& ctx) const {
        size_t compsSize = boundaryComponents.size();
        Y_ENSURE(compsSize >= 3);
        Y_ENSURE(compsSize % 2 == 1);

        TUnboxedValueVector converted;
        for (size_t i = 0; i < compsSize - 1; ++i) {
            if (i % 2 == 1) {
                converted.push_back(boundaryComponents[i]);
            }
        }
        i32 included = boundaryComponents.back().Get<i32>();
        if (included != 0) {
            included = 1;
        }
        converted.push_back(TUnboxedValuePod(included));
        return ctx.HolderFactory.VectorAsArray(converted);
    }
};

enum ERangeOp {
    RANGE_UNION,
    RANGE_INTERSECT,
    RANGE_MULTIPLY,
    RANGE_FINALIZE,
};

IComputationNode* WrapRange(ERangeOp func, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TComputationNodePtrVector lists;
    std::vector<TRangeTypeInfo> typeInfos;
    size_t listsStart = 0;
    if (func == RANGE_FINALIZE) {
        MKQL_ENSURE(callable.GetInputsCount() == 1, "Expecting single argument");
    } else if (func == RANGE_MULTIPLY) {
        MKQL_ENSURE(callable.GetInputsCount() > 1, "Expecting at least two arguments");
        listsStart = 1;
        auto limitType = callable.GetInput(0).GetStaticType();
        MKQL_ENSURE(limitType->IsData() && static_cast<TDataType*>(limitType)->GetSchemeType() == NUdf::TDataType<ui64>::Id,
            "Expecting Uint64 as first argument");
    } else {
        MKQL_ENSURE(callable.GetInputsCount() > 0, "Expecting at least one argument");
    }

    lists.reserve(callable.GetInputsCount());
    typeInfos.reserve(callable.GetInputsCount());
    for (ui32 i = listsStart; i < callable.GetInputsCount(); ++i) {
        auto type = callable.GetInput(i).GetStaticType();
        MKQL_ENSURE(type->IsList(), "Expecting list as argument");
        auto rangeType = static_cast<TListType*>(type)->GetItemType();

        if (func != RANGE_MULTIPLY) {
            MKQL_ENSURE(type->IsSameType(*callable.GetInput(listsStart).GetStaticType()), "All arguments must be of same type");
        }

        lists.push_back(LocateNode(ctx.NodeLocator, callable, i));
        typeInfos.push_back(ExtractTypes(rangeType));
    }

    switch (func) {
    case RANGE_UNION:
        return new TRangeUnionWrapper(ctx.Mutables, std::move(lists), std::move(typeInfos));
    case RANGE_INTERSECT:
        return new TRangeIntersectWrapper(ctx.Mutables, std::move(lists), std::move(typeInfos));
    case RANGE_MULTIPLY: {
        auto limit = LocateNode(ctx.NodeLocator, callable, 0);
        return new TRangeMultiplyWrapper(ctx.Mutables, limit, std::move(lists), std::move(typeInfos));
    }
    case RANGE_FINALIZE:
        return new TRangeFinalizeWrapper(ctx.Mutables, std::move(lists), std::move(typeInfos));
    default:
        Y_ENSURE(!"Unknown callable");
    }
}

class TRangeCreateWrapper : public TMutableComputationNode<TRangeCreateWrapper> {
    typedef TMutableComputationNode<TRangeCreateWrapper> TBaseComputation;
public:
    TRangeCreateWrapper(TComputationMutables& mutables, IComputationNode* list)
        : TBaseComputation(mutables)
        , List(list)
    {}

    TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValue list = List->GetValue(ctx);

        TDefaultListRepresentation res;
        TThresher<false>::DoForEachItem(list,
            [&] (NUdf::TUnboxedValue&& item) {
                auto left = ConvertToInternal(item.GetElement(0), true, ctx);
                auto right = ConvertToInternal(item.GetElement(1), false, ctx);

                TUnboxedValueVector rangeVector = { left, right };
                auto range = ctx.HolderFactory.VectorAsArray(rangeVector);
                res = res.Append(std::move(range));
            }
        );
        return ctx.HolderFactory.CreateDirectListHolder(std::move(res));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List);
    }

    TUnboxedValue ConvertToInternal(TUnboxedValue boundary, bool isLeft, TComputationContext& ctx) const {
        auto elements = boundary.GetElements();
        auto elementsCount = boundary.GetListLength();

        Y_ENSURE(elements);
        Y_ENSURE(elementsCount >= 2);

        TUnboxedValueVector converted;
        i32 included = elements[elementsCount - 1].Get<i32>();
        const auto hasPrefix = bool(elements[0]);
        bool tail = false;
        for (size_t i = 0; i < elementsCount - 1; ++i) {
            i32 infValue;
            tail = tail || !elements[i];
            if (elements[i]) {
                MKQL_ENSURE(!tail, "Invalid boundary value - non null element follows null");
                infValue = 0;
            } else {
                infValue = GetInfSign(hasPrefix, included, isLeft);
            }
            converted.push_back(TUnboxedValuePod(infValue));
            converted.push_back(elements[i]);
        }
        included = included ? (isLeft ? -1 : 1) : 0;
        converted.push_back(TUnboxedValuePod(included));
        return ctx.HolderFactory.VectorAsArray(converted);
    }

    IComputationNode* const List;
};


} // namespace

IComputationNode* WrapRangeCreate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expecting exactly one argument");

    auto list = callable.GetInput(0);

    auto itemType = static_cast<TListType*>(list.GetStaticType())->GetItemType();
    MKQL_ENSURE(itemType->IsTuple(), "Expecting list of tuples");

    auto tupleType = static_cast<TTupleType*>(itemType);
    MKQL_ENSURE(tupleType->GetElementsCount() == 2,
                "Expecting list ot 2-element tuples, got: " << tupleType->GetElementsCount() << " elements");

    MKQL_ENSURE(tupleType->GetElementType(0)->IsSameType(*tupleType->GetElementType(1)),
                "Expecting list ot 2-element tuples of same type");

    MKQL_ENSURE(tupleType->GetElementType(0)->IsTuple(),
                "Expecting range boundary to be tuple");

    auto boundaryType = static_cast<TTupleType*>(tupleType->GetElementType(0));
    MKQL_ENSURE(boundaryType->GetElementsCount() >= 2,
                "Range boundary should have at least 2 components, got: " << boundaryType->GetElementsCount());

    return new TRangeCreateWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}

IComputationNode* WrapRangeUnion(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapRange(RANGE_UNION, callable, ctx);
}

IComputationNode* WrapRangeIntersect(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapRange(RANGE_INTERSECT, callable, ctx);
}

IComputationNode* WrapRangeMultiply(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapRange(RANGE_MULTIPLY, callable, ctx);
}

IComputationNode* WrapRangeFinalize(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapRange(RANGE_FINALIZE, callable, ctx);
}

}
}
